#include "partitionedpageio.h"
#include "pageiomem.h"

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

namespace ptnk
{

PartitionedPageIO::PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode)
:	m_mode(mode), m_opts(opts), m_active(NULL), m_partidFirst(0), m_partidLast(0)
{
	PTNK_ASSERT(dbprefix != NULL && *dbprefix != '\0');
	m_dbprefix = dbprefix;
	
	scanFiles();

	if(!(opts & OWRITER))
	{
		// read only access

		if(m_parts.empty())
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

			addNewPartition();

			m_needInit = true;
		}
		else
		{
			m_needInit = false;	
		}
	}
}

PartitionedPageIO::~PartitionedPageIO()
{
	/* NOP */
}

void
PartitionedPageIO::Partition::dumpStat() const
{
	std::cout << "- partition id: " << (boost::format("%03x") % partid()) << std::endl;	
	std::cout << "  file path: " << filepath() << std::endl;	
	if(m_parthandler.get())
	{
		std::cout << "  range: " << pgid2str(PGID_PARTLOCAL(partid(), m_parthandler->getFirstPgId()))
	              << " to "      << pgid2str(PGID_PARTLOCAL(partid(), m_parthandler->getLastPgId())) << std::endl;
		m_parthandler->dumpStat();
	}
	else
	{
		std::cout << "  ! no parthandler instantiated" << std::endl;	
	}
}

void
PartitionedPageIO::Partition::instantiateHandler()
{
	// FIXME: give hint for mmap-ing
	// FIXME: instantiate as read-only if possible
	// FIXME: check magic for compacted or not

	m_parthandler.reset(new PageIOMem(filepath().c_str(), m_optsPIO, m_mode));
}

namespace
{

bool
checkperm(const char* filepath, int flags)
{
	int ret = ::open(filepath, flags);
	if(ret >= 0)
	{
		::close(ret);
		return true;
	}
	else
	{
		return false;
	}
}

} // end of anonymous namespace

void
PartitionedPageIO::scanFiles()
{
	// extract dir path from m_dbprefix
	char bufdir[4096]; bufdir[4095] = '\0';
	::strncpy(bufdir, m_dbprefix.c_str(), sizeof(bufdir)-1);
	const char* pathdir = ::dirname(bufdir);

	// extract dbname from m_dbprefix
	char bufbase[4096]; bufbase[4095] = '\0';
	::strncpy(bufbase, m_dbprefix.c_str(), sizeof(bufbase)-1);
	const char* dbname = ::basename(bufbase);
	
	// scan _pathdir_
	DIR* dir;
	PTNK_ASSURE_SYSCALL(dir = ::opendir(pathdir));

	m_partidFirst = PTNK_PARTID_MAX;
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

			Partition* p;
			m_parts.push_back(p = new Partition(partid, filepath, optsPIO, m_mode /* not used for existing file */));
			// if the part file is writable and is the newest partition, set the partition active
			if((optsPIO & OWRITER) && m_partidLast == p->partid())
			{
				m_active = p;
			}
		}
	}

	::closedir(dir);
}

PartitionedPageIO::Partition*
PartitionedPageIO::addNewPartition()
{
	part_id_t partid = ++m_partidLast;
	if(partid > PTNK_PARTID_MAX)
	{
		PTNK_THROW_RUNTIME_ERR("FIXME: handle part num > PTNK_PARTID_MAX");	
	}

	std::string filename = m_dbprefix;
	char suffix[10]; sprintf(suffix, ".%03x.ptnk", partid);
	filename.append(suffix);

	if(checkperm(filename.c_str(), 0))
	{
		PTNK_THROW_RUNTIME_ERR("wierd! the dbfile for the new partid already exists!");	
	}

	Partition* p = new Partition(partid, filename, m_opts, m_mode); 
	m_parts.push_back(p);
	m_active = p;

	return p;
}

PartitionedPageIO::Partition*
PartitionedPageIO::part(part_id_t partid)
{
	// FIXME: add cache
	
	BOOST_FOREACH(Partition& part, m_parts)
	{
		if(part.partid() == partid)	
		{
			return &part;	
		}
	}

	return NULL;
}

pair<Page, page_id_t>
PartitionedPageIO::newPage()
{
	Page pg; page_id_t pgidLocal;
	boost::tie(pg, pgidLocal) = m_active->handler()->newPage();

	page_id_t pgid = PGID_PARTLOCAL(m_active->partid(), pgidLocal);

	return make_pair(pg, pgid);
}

Page
PartitionedPageIO::readPage(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	return part(partid)->handler()->readPage(PGID_LOCALID(pgid));
}

Page
PartitionedPageIO::modifyPage(const Page& pg, mod_info_t* mod)
{
	part_id_t partid = PGID_PARTID(pg.pageId());
	return part(partid)->handler()->modifyPage(pg, mod);
}

void
PartitionedPageIO::sync(page_id_t pgid)
{
	part_id_t partid = PGID_PARTID(pgid);
	return part(partid)->handler()->sync(PGID_LOCALID(pgid));
}

void
PartitionedPageIO::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	PTNK_ASSERT(pgidStart <= pgidEnd);

	const part_id_t partEnd = PGID_PARTID(pgidEnd);
	part_id_t partStart;
	while(PTNK_UNLIKELY((partStart = PGID_PARTID(pgidStart)) != partEnd))
	{
		part(partStart)->handler()->syncRange(
			PGID_LOCALID(pgidStart), PTNK_LOCALID_INVALID
			);
		pgidStart = PGID_PARTSTART(partStart + 1);
	}

	part(partStart)->handler()->syncRange(
		PGID_LOCALID(pgidStart), PGID_LOCALID(pgidEnd)
		);
}

page_id_t
PartitionedPageIO::getLastPgId() const
{
	part_id_t partid = m_active->partid();
	local_pgid_t localid = m_active->handler()->getLastPgId();

	return PGID_PARTLOCAL(partid, localid);
}

local_pgid_t
PartitionedPageIO::getPartLastLocalPgId(part_id_t partid) const
{
	const Partition* p = part(partid);

	if(p)
	{
		return PGID_LOCALID(p->handler()->getLastPgId());
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
	// FIXME: remount old part as read-only
	if(! bForce)
	{
		// is new part. really needed?

		if(m_active->numPages() < 128 * 1024 * 1024 / 4096) // less than 128mb
		{
			return;	
		}
	}

	addNewPartition();
}

void
PartitionedPageIO::dumpStat() const
{
	std::cout << "* PartitionedPageIO stat dump *" << std::endl;
	std::cout << " - partidFirst: " << m_partidFirst << " Last: " << m_partidLast << std::endl;
	BOOST_FOREACH(const Partition& part, m_parts)
	{
		part.dumpStat();
	}
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

	std::vector<Partition*> to_delete;
	BOOST_FOREACH(Partition& part, m_parts)
	{
		if(part.partid() < partidT)
		{
			to_delete.push_back(&part);
		}
	}

	BOOST_FOREACH(Partition* part, to_delete)
	{
		discardPartition(part);
	}
}

void
PartitionedPageIO::discardPartition(Partition* p)
{
	std::string filepath = p->filepath();
	m_parts.erase_if(ptr_match<Partition>(p));

	std::cerr << "discarding old partition file: " << filepath << std::endl;
	PTNK_ASSURE_SYSCALL(::unlink(filepath.c_str()));
}

} // end of namespace ptnk
