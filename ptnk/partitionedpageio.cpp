#include "partitionedpageio.h"
#include "pageiomem.h"
#include "helperthr.h"
#include "sysutils.h"

#include <sstream>

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/types.h>
#include <libgen.h>

#include <boost/tuple/tuple.hpp>

// #define VERBOSE_PAGEIO

namespace ptnk
{

constexpr size_t NUM_PAGES_ALLOC_ONCE = 256;

constexpr unsigned long PARTSIZEFILE_MAX = 1024 * 1024 * 1024; // 1GB
constexpr unsigned long PARTSIZEFILE_MIN = 128 * 1024 * 1024; // 128MB
constexpr unsigned long PARTSIZEFILE_PREALLOC_THRESHOLD = 64 * 1024 * 1024; // 64MB  FIXME!
constexpr unsigned long PARTSIZEFILE_PREALLOC = PARTSIZEFILE_PREALLOC_THRESHOLD * 1.5;

constexpr long NPAGES_PARTMAX = PARTSIZEFILE_MAX/PTNK_PAGE_SIZE;
constexpr long NPAGES_PREALLOC_THRESHOLD = PARTSIZEFILE_PREALLOC_THRESHOLD/PTNK_PAGE_SIZE;
constexpr long NPAGES_PREALLOC = PARTSIZEFILE_PREALLOC/PTNK_PAGE_SIZE;

MappedFile*
MappedFile::createNew(part_id_t partid, const char* filename, ptnk_opts_t opts, int mode)
{
	if(!(opts & OTRUNCATE) && file_exists(filename))
	{
		std::stringstream err;
		err << "file " << filename << " already exists (and OTRUNCATE not specified)";;
		PTNK_THROW_RUNTIME_ERR(err.str());
	}

	PTNK_CHECK((opts & OWRITER) && (opts & OCREATE));

	// open file
	int fd;
	{
		int flags = O_RDWR | O_CREAT;
		if(opts & OTRUNCATE) flags |= O_TRUNC;

		PTNK_ASSURE_SYSCALL(fd = ::open(filename, flags, mode));
	}
	
	unique_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, PROT_READ | PROT_WRITE));
	mf->expandFile(NPAGES_PREALLOC);

	return mf.release();
}

MappedFile*
MappedFile::openExisting(part_id_t partid, const char* filename, ptnk_opts_t opts)
{
	// get file stat
	size_t filesize = 0;
	{
		struct stat st;
		PTNK_ASSURE_SYSCALL(::stat(filename, &st) < 0);

		if(! S_ISREG(st.st_mode))
		{
			PTNK_THROW_RUNTIME_ERR("specified dbfile is not a regular file");	
		}

		filesize = st.st_size;
	}

	// open file
	int prot = PROT_READ;
	int fd;
	{
		int flags = 0;
		if(opts & OWRITER)
		{
			flags |= O_RDWR;
			prot |= PROT_WRITE;
		}
		else
		{
			flags |= O_RDONLY;	
		}

		PTNK_ASSURE_SYSCALL(fd = ::open(filename, flags));
	}

	unique_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, prot));

	size_t pgs = filesize / PTNK_PAGE_SIZE;
	if(pgs > 0)
	{
		mf->moreMMap(pgs);
	}

	return mf.release();
}

MappedFile::MappedFile(part_id_t partid, const char* filename, int fd, int prot)
:	m_partid(partid), m_filename(filename), m_fd(fd), m_prot(prot), m_numPagesReserved(0)
{
	m_mapFirst.pgidEnd = 0;
	m_mapFirst.offset = NULL;

	m_isReadOnly = (prot == PROT_READ);
}

MappedFile::~MappedFile()
{
	// unmap mmap(2)s
	page_id_t prevEnd = 0;
	for(Mapping* m = &m_mapFirst; m; m = m->next.get())
	{
		if(m->offset)
		{
			size_t len = (m->pgidEnd - prevEnd) * PTNK_PAGE_SIZE;
			::munmap(m->offset, len);
		}

		prevEnd = m->pgidEnd;
	}
	
	// close file
	if(m_fd > 0)
	{
		::close(m_fd);	
	}
}

MappedFile::Mapping*
MappedFile::getmLast()
{
	Mapping* p = &m_mapFirst, *np = NULL;
	while((np = p->next.get()))
	{
		p = np;
	}

	return p;
}

void
MappedFile::moreMMap(size_t pgs)
{
	MUTEXPROF_START("moreMMap");
	Mapping* mLast = getmLast();

	static char* s_lastmapend = PTNK_MMAP_HINT;
	char* mapstart;
	char* hint;
	if(mLast->offset)
	{
		hint = mLast->offset + mLast->pgidEnd * PTNK_PAGE_SIZE;
	}
	else
	{
		hint = s_lastmapend;
	}

	if(isFile())
	{
		PTNK_ASSURE_SYSCALL_NEQ(
			mapstart = static_cast<char*>(::mmap(hint, pgs * PTNK_PAGE_SIZE, m_prot, MAP_SHARED, m_fd, m_numPagesReserved * PTNK_PAGE_SIZE)),
			MAP_FAILED
			)
		{
			std::cerr << "m_fd: " << m_fd<< std::endl;	
		};
	}
	else
	{
		PTNK_ASSURE_SYSCALL_NEQ(
			mapstart = static_cast<char*>(::mmap(hint, pgs * PTNK_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, m_fd, 0)),
			MAP_FAILED
			)
		{
			std::cerr << "m_fd: " << m_fd<< std::endl;	
		};
	}
	PTNK_CHECK(mapstart);
	s_lastmapend = mapstart + PTNK_PAGE_SIZE*pgs;

	if(!mLast->offset || mapstart == hint)
	{
		// newly mapped region contiguous to current one
		if(! mLast->offset) mLast->offset = mapstart;
		mLast->pgidEnd += pgs;

		PTNK_MEMBARRIER_COMPILER;

		m_numPagesReserved = mLast->pgidEnd;
	}
	else
	{
		// add new mapping
		unique_ptr<Mapping> mNew(new Mapping);

		mNew->pgidEnd = mLast->pgidEnd + pgs;
		mNew->offset = mapstart - mLast->pgidEnd * PTNK_PAGE_SIZE;

		PTNK_MEMBARRIER_COMPILER;
		
		mLast->next = move(mNew);

		PTNK_MEMBARRIER_COMPILER;

		m_numPagesReserved = mLast->next->pgidEnd;
	}
	MUTEXPROF_END;
}

size_t
MappedFile::expandFile(size_t pgs)
{
	#ifdef VERBOSE_PAGEIO
	std::cout << "expandFile " << pgs << " pgs -> ";
	#endif
	if(pgs < NUM_PAGES_ALLOC_ONCE) pgs = NUM_PAGES_ALLOC_ONCE;
	if(pgs + m_numPagesReserved > (long)NPAGES_PARTMAX) pgs = NPAGES_PARTMAX - m_numPagesReserved;
	#ifdef VERBOSE_PAGEIO
	std::cout << pgs << " pgs" << std::endl;
	#endif
	if(pgs == 0) return 0;

	// expand file size
	if(isFile())
	{
		MUTEXPROF_START("fallocate");
		size_t allocsize = pgs * PTNK_PAGE_SIZE;
	#ifdef USE_POSIX_FALLOCATE
	 	int ret;
	 	if(0 != (ret = ::posix_fallocate(m_fd, m_numPagesReserved * PTNK_PAGE_SIZE, allocsize)))
	 	{
	 		throw ptnk_syscall_error(__FILE__, __LINE__, "posix_fallocate", ret);
	 	}
	#elif defined(USE_FTRUNCATE)
		(void) allocsize;
		PTNK_ASSURE_SYSCALL(::ftruncate(m_fd, (m_numPagesReserved + pgs) * PTNK_PAGE_SIZE));
	#elif defined(USE_PWRITE)
		// expand file first
		unique_ptr<char> buf(new char[allocsize + PTNK_PAGE_SIZE]);
		PTNK_ASSURE_SYSCALL(::pwrite(m_fd, buf.get(), allocsize, m_numPagesReserved * PTNK_PAGE_SIZE));
		PTNK_ASSURE_SYSCALL(::fsync(m_fd));
	#else
		#error no file expand method defined
	#endif
		MUTEXPROF_END;
	}

	// mmap expanded region
	moreMMap(pgs);

	return pgs;
}

void
MappedFile::sync(local_pgid_t pgidStart, local_pgid_t pgidEnd)
{
	if(! isFile()) return; // no need to sync when not mapped to file

#ifdef PTNK_FDATASYNC
	PTNK_ASSURE_SYSCALL(::fdatasync(m_fd));
	return;
#endif

	if(pgidEnd > m_numPagesReserved) pgidEnd = m_numPagesReserved;

#ifdef PTNK_SYNC_FILE_RANGE
	loff_t off = ((loff_t)pgidStart) * PTNK_PAGE_SIZE;
	loff_t len = ((loff_t)(pgidEnd - pgidStart + 1)) * PTNK_PAGE_SIZE;
	if(len < 0) return;
	PTNK_ASSURE_SYSCALL(::sync_file_range(m_fd, off, len, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER))
	{
		std::cout << "syncRange " << pgid2str(pgidStart) << " to " << pgid2str(pgidEnd) << std::endl;
		std::cout << "m_fd: " << m_fd << std::endl;
		std::cout << "off: " << off << " len: " << len << std::endl;
	}
#else
	Mapping* m = &m_mapFirst;
	while(m)
	{
		if(pgidEnd > m->pgidEnd)
		{
			intptr_t off = reinterpret_cast<intptr_t>(m->offset) + PTNK_PAGE_SIZE * pgidStart;
			size_t len = PTNK_PAGE_SIZE * (m->pgidEnd - pgidStart);
			PTNK_ASSURE_SYSCALL(::msync((void*)off, len, MS_SYNC));

			pgidStart = m->pgidEnd;
			m = m->next.get();
		}
		else
		{
			intptr_t off = reinterpret_cast<intptr_t>(m->offset) + PTNK_PAGE_SIZE * pgidStart;
			size_t len = PTNK_PAGE_SIZE * (pgidEnd - pgidStart + 1);
			PTNK_ASSURE_SYSCALL(::msync((void*)off, len, MS_SYNC));

			break;	
		}
	}
#endif
}

void
MappedFile::makeReadOnly()
{
	m_isReadOnly = true;

	// FIXME: re mmap region as read-only
	//   but this has to be delayed, as newPart() calls this only to prevent further allocation of pages and for NOT TO stop writes into previously allocated pages from this partition
}

void
MappedFile::dump(std::ostream& o) const
{
	o << std::setfill('0');
	o << "- partition id: " << std::hex << std::setw(3) << partid() << std::dec << std::endl;	
	o << std::setfill(' ');
	o << "  filename: " << m_filename << std::endl;	

	const Mapping* m = &m_mapFirst;
	while(m)
	{
		o << "  | map pgidEnd: " << m->pgidEnd << " offset: " << (void*)m->offset << std::endl;
		m = m->next.get();
	}
}

void
MappedFile::discardFile()
{
	std::cerr << "discarding old partition file: " << m_filename << std::endl;
	PTNK_ASSURE_SYSCALL(::unlink(m_filename.c_str()));
}

PartitionedPageIO::PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode)
:	m_mode(mode), m_opts(opts),
    m_pgidLast(PGID_INVALID),
	m_partidFirst(PTNK_PARTID_INVALID), m_partidLast(0),
	m_helper(nullptr), m_isHelperInvoked(true)
{
	PTNK_CHECK(dbprefix != NULL && *dbprefix != '\0');
	m_dbprefix = dbprefix;
	
	MappedFile* latest = openFiles();

	if(!(opts & OWRITER))
	{
		// read only access

		if(! latest)
		{
			PTNK_THROW_RUNTIME_ERR("no existing dbpart file found");
		}
	}
	else
	{
		// read/write access

		if(! latest)
		{
			if(!(opts & OCREATE))
			{
				PTNK_THROW_RUNTIME_ERR("opts OWRITER specified but no existing dbfile found. also specify OCREATE to create new dbfiles.");	
			}

			addNewPartition_unsafe();

			m_needInit = true;
		}
		else
		{
			scanLastPgId(latest->partid()); // set m_pgidLast to correct value
			m_needInit = false;	
		}
	}
}

void
PartitionedPageIO::attachHelper(Helper* helper)
{
	PTNK_CHECK(! m_helper);
	PTNK_CHECK(helper);

	m_helper = helper;
	m_isHelperInvoked = false;
}

PartitionedPageIO::~PartitionedPageIO()
{
	/* NOP */
}

void
PartitionedPageIO::drop(const char* dbprefix)
{
	Vpartfile_t files; scanFiles(&files, dbprefix);

	for(auto fp_partid: files)
	{
		PTNK_ASSURE_SYSCALL(::unlink(fp_partid.first.c_str()));
	}
}

void
PartitionedPageIO::scanFiles(Vpartfile_t* files, const char* dbprefix)
{
	// extract dir path from dbprefix
	char bufdir[4096]; bufdir[4095] = '\0';
	::strncpy(bufdir, dbprefix, sizeof(bufdir)-1);
	const char* pathdir = ::dirname(bufdir);

	// extract dbname from dbprefix
	char bufbase[4096]; bufbase[4095] = '\0';
	::strncpy(bufbase, dbprefix, sizeof(bufbase)-1);
	const char* dbname = ::basename(bufbase);
	
	// scan _pathdir_
	DIR* dir;
	PTNK_ASSURE_SYSCALL_NEQ(dir = ::opendir(pathdir), NULL);

	struct dirent* entry;
	while((entry = ::readdir(dir)) != NULL)
	{
		const char* filename = entry->d_name;

		// stat(2) file info
		struct stat st;
		// - create full path str
		std::string filepath(pathdir);
		filepath.append("/");
		filepath.append(filename);

		// - stat(2)!
		PTNK_ASSURE_SYSCALL(::stat(filepath.c_str(), &st));

		// filter non-regular file
		if(! S_ISREG(st.st_mode)) continue;

		// filter db prefix
		size_t dbnamelen = ::strlen(dbname);
		if(::strncmp(filename, dbname, dbnamelen) != 0) continue;

		// check and parse format
		// - check suffix len
		size_t fnlen = ::strlen(filename);
		if(fnlen - dbnamelen != /* ::strlen(".XXX.ptnk")-1 */ 9) continue;

		// - check first dot and fileext
		if(filename[dbnamelen] != '.') continue;
		if(::strncmp(&filename[dbnamelen + 4], ".ptnk", 6) != 0) continue;

		// - check hex
		char hex[4]; hex[3] = '\0';
		bool valid = true;
		for(int i = 0; i < 3; ++ i)
		{
			char c = filename[dbnamelen + /* '.' */ 1 + i];
			// check c =~ /[0-9a-zA-Z]/
			if(!(
				('0' <= c && c <= '9')
			 || ('a' <= c && c <= 'f')
			 || ('A' <= c && c <= 'F')
				))
			{
				valid = false; break;
			}

			hex[i] = c;
		}
		if(! valid) continue;
		
		// - parse hex -> part_id
		part_id_t partid = static_cast<part_id_t>(::strtol(hex, NULL, 16));
		if(partid > PTNK_PARTID_MAX) continue;

		// std::cout << "dbfile found: " << filepath << " part_id: " << partid << std::endl;
		files->push_back(make_pair(filepath, partid));
	}

	::closedir(dir);
}

MappedFile*
PartitionedPageIO::openFiles()
{
	MappedFile* active = NULL;

	Vpartfile_t files; scanFiles(&files, m_dbprefix.c_str());

	m_partidFirst = PTNK_PARTID_INVALID;
	for(const auto& fp_partid: files)
	{
		const std::string& filepath = fp_partid.first;
		const part_id_t partid = fp_partid.second;

		if(m_opts & OTRUNCATE)
		{
			std::cout << "  O_TRUNCATE has been specified. deleting: " << filepath << std::endl;

			// delete existing db part file
			PTNK_ASSURE_SYSCALL(::unlink(filepath.c_str()));
		}
		else
		{
			if(partid < m_partidFirst) m_partidFirst = partid;
			if(partid > m_partidLast) m_partidLast = partid;

			ptnk_opts_t optsPIO = m_opts;

			// check permission
			if((optsPIO & OWRITER) && !checkperm(filepath.c_str(), O_RDWR))
			{
				optsPIO &= ~(ptnk_opts_t)OWRITER;
			}

			MappedFile* p;
			m_parts[partid].reset(p = MappedFile::openExisting(partid, filepath.c_str(), optsPIO));

			// if the part file is writable and is the newest partition, set the partition active
			if((optsPIO & OWRITER) && m_partidLast == p->partid())
			{
				active = p;
			}
		}
	}

	return active;
}

void
PartitionedPageIO::scanLastPgId(part_id_t partidLatest)
{
	// find last committed pg
	
	// FIXME: partid wrap around not considered!!!
	for(part_id_t partid = partidLatest; partid != PTNK_PARTID_INVALID; -- partid) 
	{
		std::cout << "scan partid: " << std::hex << partid << std::endl;

		MappedFile* part = m_parts[partid].get();
		PTNK_CHECK(part);
		for(local_pgid_t pgidL = part->numPagesReserved() - 1; pgidL != ~0UL; -- pgidL)
		{
			page_id_t pgid = PGID_PARTLOCAL(part->partid(), pgidL);
			Page pg(readPage(pgid));
			// std::cout << "scan pg " << pgid2str(pgid) << " commited: " << pg.isCommitted() << std::endl;
			if(pg.isCommitted())
			{
				m_pgidLast = PGID_PARTLOCAL(part->partid(), pgidL);
				return;
			}
		}
	}
}

void
PartitionedPageIO::addNewPartition_unsafe()
{
	part_id_t partid;
	if(m_partidFirst == PTNK_PARTID_INVALID)
	{
		// first partition
		m_partidFirst = m_partidLast = partid = 0;
	}
	else
	{
		partid = ++m_partidLast;
	}

	if(partid > PTNK_PARTID_MAX)
	{
		PTNK_THROW_RUNTIME_ERR("FIXME: handle part num > PTNK_PARTID_MAX");	
	}

	std::string filename = m_dbprefix;
	char suffix[10]; sprintf(suffix, ".%03x.ptnk", partid);
	filename.append(suffix);

	if(file_exists(filename.c_str()))
	{
		PTNK_THROW_RUNTIME_ERR("weird! the dbfile for the new partid already exists!");	
	}

	m_parts[partid] = unique_ptr<MappedFile>(MappedFile::createNew(partid, filename.c_str(), m_opts, m_mode));
}

void
PartitionedPageIO::expandTo(page_id_t pgid)
{
	boost::unique_lock<boost::mutex> g(m_mtxAlloc);

	#ifdef VERBOSE_PAGEIO
	std::cout << "old pgid max: " << pgid2str(PGID_PARTLOCAL(m_partidLast, m_parts[m_partidLast]->numPagesReserved())) << std::endl;
	#endif

	for(;;)
	{
		part_id_t partid = PGID_PARTID(pgid);

		MappedFile* part = m_parts[partid].get();
		if(! part)
		{
			// need new partition
			
			PTNK_CHECK(partid-1 == m_partidLast);
			addNewPartition_unsafe();
			continue;
		}

		ssize_t numNeeded = PGID_LOCALID(pgid) - part->numPagesReserved() + 1;
		if(numNeeded <= 0) break;

		// try expanding current partition
		numNeeded -= part->expandFile(numNeeded);

		if(numNeeded <= 0) break;

		pgid = PGID_PARTLOCAL(partid+1, numNeeded-1);
	}

	#ifdef VERBOSE_PAGEIO
	std::cout << "new pgid max: " << pgid2str(PGID_PARTLOCAL(m_partidLast, m_parts[m_partidLast]->numPagesReserved())) << std::endl;
	#endif
}

pair<Page, page_id_t>
PartitionedPageIO::newPage()
{
	page_id_t pgidLast, pgid;
	do
	{
	RETRY:
		pgidLast = m_pgidLast;
		static_assert(PGID_INVALID + 1 == 0, "below code assumes this");

		pgid = pgidLast + 1;

	TRYTHISPGID:
		part_id_t partid = PGID_PARTID(pgid);
		MappedFile* part = m_parts[partid].get();
		if(! part)
		{
		#ifdef VERBOSE_PAGEIO
			std::cout << "during forced new-part alloc OR file for part does not exist" << std::endl;
		#endif
			expandTo(pgid);
			goto RETRY;
		}

		if(part->isReadOnly())
		{
			// try next partition

			pgid = PGID_PARTLOCAL(partid+1, 0);	
			goto TRYTHISPGID;
		}

		ssize_t numNeeded = PGID_LOCALID(pgid) - part->numPagesReserved() + 1;
		if(numNeeded > 0)
		{
			if(m_parts[partid+1])
			{
				// try next partition

				pgid = PGID_PARTLOCAL(partid+1, 0);	
				goto TRYTHISPGID;
			}
			else
			{
				// need more pages...
			#ifdef VERBOSE_PAGEIO
				std::cout << "running out of space: " << pgid2str(pgid) << std::endl;
			#endif
				
				MUTEXPROF_START("expand stall");
				expandTo(pgid);
				MUTEXPROF_END;
				goto RETRY;
			}
		}

#if 1
		// preallocate helper
		if(!m_isHelperInvoked && ! m_parts[partid+1] && numNeeded > -NPAGES_PREALLOC_THRESHOLD)
		{
			// make helper pre-allocate pages
			if(PTNK_CAS(&m_isHelperInvoked, false, true))
			{
				m_helper->enq([this, pgid]() {
				#ifdef VERBOSE_PAGEIO
					std::cout << "pre alloc to " << pgid2str(pgid+NPAGES_PREALLOC) << std::endl;
				#endif
					expandTo(pgid + NPAGES_PREALLOC);
				#ifdef VERBOSE_PAGEIO
					std::cout << "pre alloc done" << std::endl;
				#endif

					m_isHelperInvoked = false;	
				});
			}
		}
#endif
	}
	while(! PTNK_CAS(&m_pgidLast, pgidLast, pgid));

	return make_pair(Page(m_parts[PGID_PARTID(pgid)]->calcPtr(PGID_LOCALID(pgid)), true), pgid);
}

Page
PartitionedPageIO::readPage(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	return Page(m_parts[partid]->calcPtr(PGID_LOCALID(pgid)), false);
}

void
PartitionedPageIO::sync(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	local_pgid_t pgidL = PGID_LOCALID(pgid);
	return m_parts[partid]->sync(pgidL, pgidL);
}

void
PartitionedPageIO::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	PTNK_ASSERT(pgidStart <= pgidEnd);

	const part_id_t partEnd = PGID_PARTID(pgidEnd);
	part_id_t partStart;
	while(PTNK_UNLIKELY((partStart = PGID_PARTID(pgidStart)) != partEnd))
	{
		if(! m_parts[partStart]) return;

		m_parts[partStart]->sync(
			PGID_LOCALID(pgidStart), PTNK_LOCALID_INVALID
			);
		pgidStart = PGID_PARTSTART(partStart + 1);
	}

	m_parts[partStart]->sync(
		PGID_LOCALID(pgidStart), PGID_LOCALID(pgidEnd)
		);
}

page_id_t
PartitionedPageIO::getLastPgId() const
{
	return m_pgidLast;
}

local_pgid_t
PartitionedPageIO::getPartLastLocalPgId(part_id_t partid) const
{
	MappedFile* part = m_parts[partid].get();
	if(part)
	{
		return PGID_LOCALID(part->numPagesReserved()-1);
	}
	else
	{
		return PTNK_LOCALID_INVALID;
	}
}

bool
PartitionedPageIO::needInit() const
{
	return m_needInit;
}

void
PartitionedPageIO::newPart(bool bForce)
{
	// is new part. really needed?
	if(! bForce && m_parts[m_partidLast]->numPagesReserved() < PARTSIZEFILE_MIN/PTNK_PAGE_SIZE) return;	

	// FIXME: remount old part as read-only

	boost::unique_lock<boost::mutex> g(m_mtxAlloc);
	
	// re-check! (new part may be created while waiting on m_mtxAlloc)
	MappedFile* oldpart = m_parts[m_partidLast].get();
	if(! bForce && oldpart->numPagesReserved() < PARTSIZEFILE_MIN/PTNK_PAGE_SIZE) return;

	addNewPartition_unsafe();
	oldpart->makeReadOnly();
}

void
PartitionedPageIO::dumpStat() const
{
	std::cout << *this;
}

void
PartitionedPageIO::dump(std::ostream& s) const
{
	s << "* PartitionedPageIO stat dump *" << std::endl;
	s << "# pgidLast: " << pgid2str(getLastPgId()) << std::endl;
	s << "# partidFirst: " << m_partidFirst << " Last: " << m_partidLast << std::endl;
	for(auto& part: m_parts)
	{
		if(part) s << *part;
	}
}

size_t
PartitionedPageIO::numPartitions_() const
{
	size_t ret = 0;	

	for(auto& part: m_parts)
	{
		if(part) ++ ret;
	}

	return ret;
}

page_id_t
PartitionedPageIO::alignCompactionThreshold(page_id_t threshold) const
{
	part_id_t partid = PGID_PARTID(threshold);
	if(PGID_LOCALID(threshold) != 0)
	{
		-- partid;
	}

	if(partid < m_partidFirst)
	{
		return PGID_INVALID;
	}

	return PGID_PARTLOCAL(partid, 0);
}

void
PartitionedPageIO::discardOldPages(page_id_t threshold)
{
	threshold = alignCompactionThreshold(threshold);
	if(threshold == PGID_INVALID) return;

	part_id_t partidT = PGID_PARTID(threshold);

	// FIXME: won't work if partid wrap around
	for(part_id_t id = 0; id < partidT; ++ id)
	{
		MappedFile* part = m_parts[id].get();
		if(! part) continue;

		part->discardFile();
		
		m_parts[id].reset();
	}
}

} // end of namespace ptnk
