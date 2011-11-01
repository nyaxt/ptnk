#include "partitionedpageio.h"
#include "pageiomem.h"
#include "sysutils.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/types.h>
#include <libgen.h>

#include <boost/foreach.hpp>
#include <boost/tuple/tuple.hpp>

// #define VERBOSE_PAGEIO

namespace ptnk
{

constexpr unsigned long PARTSIZEFILE_MAX = 1024 * 1024 * 1024; // 1GB
constexpr unsigned long PARTSIZEFILE_MIN = 128 * 1024 * 1024; // 128MB

MappedFile*
MappedFile::createNew(part_id_t partid, const char* filename, ptnk_opts_t opts, int mode)
{
	if(!(opts & OTRUNCATE) && file_exists(filename))
	{
		PTNK_THROW_RUNTIME_ERR((boost::format("file %1% already exists (and OTRUNCATE not specified)") % filename).str());
	}

	PTNK_CHECK((opts & OWRITER) && (opts & OCREATE));

	// open file
	int fd;
	{
		int flags = O_RDWR | O_CREAT;
		if(opts & OTRUNCATE) flags |= O_TRUNC;

		PTNK_ASSURE_SYSCALL(fd = ::open(filename, flags, mode));
	}
	
	std::auto_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, PROT_READ | PROT_WRITE));
	mf->expandFile(1024);

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

	std::auto_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, prot));

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
}

MappedFile::~MappedFile()
{
	/* NOP */
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

	char* mapstart;
	char* hint = (mLast->offset != NULL) ? mLast->offset + mLast->pgidEnd * PTNK_PAGE_SIZE : PTNK_MMAP_HINT;

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
	PTNK_ASSERT(mapstart);

	if(mapstart == hint)
	{
		// newly mapped region contiguous to current one...
		
		m_numPagesReserved = mLast->pgidEnd = mLast->pgidEnd + pgs;
		if(! mLast->offset) mLast->offset = mapstart;
	}
	else
	{
		std::auto_ptr<Mapping> mNew(new Mapping);

		m_numPagesReserved = mNew->pgidEnd = mLast->pgidEnd + pgs;
		mNew->offset = mapstart - mLast->pgidEnd * PTNK_PAGE_SIZE;
		
		mLast->next = mNew; // move semantics
	}
	MUTEXPROF_END;
}

void
MappedFile::expandFile(size_t pgs)
{
	if(pgs < NUM_PAGES_ALLOC_ONCE) pgs = NUM_PAGES_ALLOC_ONCE;

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
	#else
		// expand file first
		boost::scoped_ptr<char> buf(new char[allocsize + PTNK_PAGE_SIZE]);
		PTNK_ASSURE_SYSCALL(::pwrite(m_fd, buf.get(), allocsize, m_numPagesReserved * PTNK_PAGE_SIZE));
		PTNK_ASSURE_SYSCALL(::fsync(m_fd));
	#endif
		MUTEXPROF_END;
	}

	// mmap expanded region
	moreMMap(pgs);
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
		if(pgidEnd < m->pgidEnd)
		{
			size_t off = m->offset + PTNK_PAGE_SIZE * pgidStart;
			size_t len = PTNK_PAGE_SIZE * (m->pgidEnd - pgidStart);
			PTNK_ASSURE_SYSCALL(::msync(off, len, MS_SYNC | MS_INVALIDATE));

			pgidStart = m->pgidEnd;
			m = m->next.get();
			if(! m) break;
		}
		else
		{
			size_t off = m->offset + PTNK_PAGE_SIZE * pgidStart;
			size_t len = PTNK_PAGE_SIZE * (pgidEnd- pgidStart + 1);
			PTNK_ASSURE_SYSCALL(::msync(off, len, MS_SYNC | MS_INVALIDATE));

			break;	
		}
	}
#endif
}

void
MappedFile::dumpStat() const
{
	std::cout << "- partition id: " << (boost::format("%03x") % partid()) << std::endl;	
	std::cout << "  filename: " << m_filename << std::endl;	
}

void
MappedFile::discardFile()
{
	std::cerr << "discarding old partition file: " << m_filename << std::endl;
	PTNK_ASSURE_SYSCALL(::unlink(m_filename.c_str()));
}

PartitionedPageIO::PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode)
:	m_mode(mode), m_opts(opts), m_active(NULL), m_pgidLNext(0), m_partidFirst(0), m_partidLast(0)
{
	PTNK_ASSERT(dbprefix != NULL && *dbprefix != '\0');
	m_dbprefix = dbprefix;
	
	openFiles();

	if(!(opts & OWRITER))
	{
		// read only access

		if(! m_active)
		{
			PTNK_THROW_RUNTIME_ERR("no existing dbpart file found");
		}
	}
	else
	{
		// read/write access

		if(! m_active)
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
			scanLastPgId(); // set m_pgidLNext correct value
			m_needInit = false;	
		}
	}
}

PartitionedPageIO::~PartitionedPageIO()
{
	/* NOP */
}

void
PartitionedPageIO::drop(const char* dbprefix)
{
	Vpartfile_t files; scanFiles(&files, dbprefix);

	std::string filepath; part_id_t _;
	BOOST_FOREACH(boost::tie(filepath, _), files)
	{
		PTNK_ASSURE_SYSCALL(::unlink(filepath.c_str()));
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

void
PartitionedPageIO::openFiles()
{
	Vpartfile_t files; scanFiles(&files, m_dbprefix.c_str());

	m_partidFirst = PTNK_PARTID_MAX;
	std::string filepath; part_id_t partid;
	BOOST_FOREACH(boost::tie(filepath, partid), files)
	{
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
			p = MappedFile::openExisting(partid, filepath.c_str(), optsPIO);
			m_parts.replace(partid, p);

			// if the part file is writable and is the newest partition, set the partition active
			if((optsPIO & OWRITER) && m_partidLast == p->partid())
			{
				m_active = p;
			}
		}
	}
}

void
PartitionedPageIO::scanLastPgId()
{
	// find last committed pg
	for(local_pgid_t pgidL = m_active->numPagesReserved() - 1; pgidL != ~0UL; -- pgidL)
	{
		Page pg(readPage(PGID_PARTLOCAL(m_active->partid(), pgidL)));
		if(pg.isCommitted())
		{
			m_pgidLNext = pgidL + 1;
			break;
		}
	}
}

MappedFile*
PartitionedPageIO::addNewPartition_unsafe()
{
	part_id_t partid;
	if(! m_active)
	{
		// first partition
		partid = 0;
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

	MappedFile* p = m_active = MappedFile::createNew(partid, filename.c_str(), m_opts, m_mode);
	m_parts.replace(partid, p);

	return p;
}

pair<Page, page_id_t>
PartitionedPageIO::newPage()
{
	local_pgid_t pgidL;

RETRY:
	pgidL = m_pgidLNext;
	if(pgidL >= m_active->numPagesReserved())
	{
		// need more pages...
#ifdef VERBOSE_PAGEIO
		std::cout << "running out of space" << std::endl;
#endif
		boost::unique_lock<boost::mutex> g(m_mtxAlloc);

		if(m_pgidLNext >= PARTSIZEFILE_MAX / PTNK_PAGE_SIZE)
		{
			addNewPartition_unsafe();
			m_pgidLNext = 0;
			goto RETRY;
		}

		// make sure that other thread has not already alloced pages
		ssize_t numNeeded = m_pgidLNext - m_active->numPagesReserved() + 1;
		if(numNeeded > 0)
		{
#ifdef VERBOSE_PAGEIO
			std::cout << "I'm the one going to alloc!!!: " << boost::this_thread::get_id() << std::endl;
#endif
			// do alloc
			m_active->expandFile(numNeeded);
		}
		
		goto RETRY;
	}

	if(! __sync_bool_compare_and_swap(&m_pgidLNext, pgidL, pgidL+1))
	{
		// reservation failed

		goto RETRY;
	}

	page_id_t pgid = PGID_PARTLOCAL(m_active->partid(), pgidL);
	return make_pair(Page(m_active->calcPtr(pgidL), true), pgid);
}

Page
PartitionedPageIO::readPage(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	return Page(m_parts[partid].calcPtr(PGID_LOCALID(pgid)), false);
}

void
PartitionedPageIO::sync(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	local_pgid_t pgidL = PGID_LOCALID(pgid);
	return m_parts[partid].sync(pgidL, pgidL);
}

void
PartitionedPageIO::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	PTNK_ASSERT(pgidStart <= pgidEnd);

	const part_id_t partEnd = PGID_PARTID(pgidEnd);
	part_id_t partStart;
	while(PTNK_UNLIKELY((partStart = PGID_PARTID(pgidStart)) != partEnd))
	{
		if(m_parts.is_null(partStart)) return;

		m_parts[partStart].sync(
			PGID_LOCALID(pgidStart), PTNK_LOCALID_INVALID
			);
		pgidStart = PGID_PARTSTART(partStart + 1);
	}

	m_parts[partStart].sync(
		PGID_LOCALID(pgidStart), PGID_LOCALID(pgidEnd)
		);
}

page_id_t
PartitionedPageIO::getLastPgId() const
{
	part_id_t partid = m_active->partid();
	local_pgid_t localid = m_pgidLNext - 1;

	return PGID_PARTLOCAL(partid, localid);
}

local_pgid_t
PartitionedPageIO::getPartLastLocalPgId(part_id_t partid) const
{
	if(partid == m_active->partid())
	{
		return m_pgidLNext - 1;	
	}

	if(! m_parts.is_null(partid))
	{
		// FIXME: this is not always correct
		return PGID_LOCALID(m_parts[partid].numPagesReserved()-1);
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
	if(! bForce && m_active->numPagesReserved() < PARTSIZEFILE_MIN / PTNK_PAGE_SIZE) return;	

	// FIXME: remount old part as read-only

	boost::unique_lock<boost::mutex> g(m_mtxAlloc);
	m_pgidLNext = PTNK_LOCALID_INVALID; // force later newPage() call to wait on m_mtxAlloc

	addNewPartition_unsafe();
	m_pgidLNext = 0;
}

void
PartitionedPageIO::dumpStat() const
{
	std::cout << "* PartitionedPageIO stat dump *" << std::endl;
	std::cout << " - partidFirst: " << m_partidFirst << " Last: " << m_partidLast << std::endl;
	BOOST_FOREACH(const MappedFile& part, m_parts)
	{
		if(&part) part.dumpStat();
	}
}

size_t
PartitionedPageIO::numPartitions_() const
{
	size_t ret = 0;	

	BOOST_FOREACH(const MappedFile& part, m_parts)
	{
		if(&part) ++ ret;
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
		MappedFile& part = m_parts[id];
		if(! &part) continue;

		part.discardFile();
		
		m_parts.replace(id, NULL);
	}
}

} // end of namespace ptnk
