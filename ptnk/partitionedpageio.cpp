#include "partitionedpageio.h"
#include "mappedfile.h"
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
#include <libgen.h> // for basename / dirname

namespace ptnk
{

PartitionedPageIO::PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode)
:	m_mode(mode), m_opts(opts),
    m_pgidLast(PGID_INVALID),
	m_partidFirst(PTNK_PARTID_INVALID), m_partidLast(0),
	m_helper(nullptr), m_isHelperInvoked(true)
{
	PTNK_ASSERT(!strempty(dbprefix) && (opts & OPARTITIONED));

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
	MappedFile* active = nullptr;

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
			m_parts[partid].reset(p = MappedFile::openExisting(partid, filepath, optsPIO));

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

	m_parts[partid] = unique_ptr<MappedFile>(MappedFile::createNew(partid, filename, m_opts, m_mode));
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
