#include "pageiomem.h"
#include "mappedfile.h"
#include "helperthr.h"
#include "sysutils.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace ptnk
{

// #define VERBOSE_PAGEIOMEM

PageIOMem::PageIOMem(const char* filename, ptnk_opts_t opts, int mode)
:	m_pgidLast(PGID_INVALID), m_sync(opts & OAUTOSYNC)
{
#ifdef VERBOSE_PAGEIOMEM
	std::cout << "new PageIOMem" << std::endl;
#endif

	if(strempty(filename))
	{
		// in-mem db
		PTNK_CHECK_CMNT(opts & OWRITER, "in-mem db can't be used read-only");
		m_mf = std::unique_ptr<MappedFile>(MappedFile::createMem());

		m_isFile = false;
		m_needInit = true;
	}
	else
	{
		// attached to file
		m_isFile = true;

		// get file stat
		bool doesExist = false;
		{
			struct stat st;
			if(::stat(filename, &st) < 0)
			{
				if(errno == ENOENT)
				{
					doesExist = false;
				}
				else
				{
					PTNK_ASSURE_SYSCALL(::stat(filename, &st));
				}
			}
			else
			{
				if(! S_ISREG(st.st_mode))
				{
					PTNK_THROW_RUNTIME_ERR("specified dbfile is not a regular file");	
				}
				doesExist = true;
			}
		}
#ifdef VERBOSE_PAGEIOMEM
		std::cout << "PageIOMem filename:" << filename << " doesExist: " << doesExist << " filesize: " << filesize << std::endl;
#endif

		// create file mapping:
		// - new file
		// - use existing file

		if(! doesExist || (opts & OTRUNCATE))
		{
			PTNK_CHECK_CMNT(opts & OCREATE, "file does not exist or OTRUNCATE specified, but OCREATE option is not specified");
			m_mf = unique_ptr<MappedFile>(MappedFile::createNew(0, filename, opts, mode));
			m_needInit = true;	
		}
		else
		{
			m_mf = unique_ptr<MappedFile>(MappedFile::openExisting(0, filename, opts));
			m_needInit = false;	
		}
	}

	if(! m_needInit)
	{
		scanLastPgId();	
	}

#ifdef VERBOSE_PAGEIOMEM
	dumpStat();
#endif
}

PageIOMem::~PageIOMem()
{
	/* NOP */
}

void
PageIOMem::expandTo(page_id_t pgid)
{
	std::lock_guard<std::mutex> g(m_mtxAlloc);

	ssize_t numNeeded = static_cast<ssize_t>(pgid) - m_mf->numPagesReserved() + 1;
	if(numNeeded <= 0) return;

	m_mf->expandFile(numNeeded);
}

pair<Page, page_id_t>
PageIOMem::newPage()
{
	page_id_t pgidLast, pgid;
	do
	{
	RETRY:
		pgidLast = m_pgidLast;
		static_assert(PGID_INVALID + 1 == 0, "below code assumes this");

		pgid = pgidLast + 1;

		ssize_t numNeeded = static_cast<ssize_t>(pgid) - m_mf->numPagesReserved() + 1;
		if(numNeeded > 0)
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

#if 0
		// preallocate helper
		if(!m_isHelperInvoked && numNeeded > -NPAGES_PREALLOC_THRESHOLD)
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

	return make_pair(Page(m_mf->calcPtr(pgid), true), pgid);
}

Page
PageIOMem::readPage(page_id_t pgid)
{
	PTNK_ASSERT(pgid != PGID_INVALID);
	return Page(m_mf->calcPtr(pgid), false);
}

void
PageIOMem::sync(page_id_t pgid)
{
	if(!m_sync || !m_isFile) return;

	m_mf->sync(pgid, pgid);
}

void
PageIOMem::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	if(!m_sync || !m_isFile) return;

	m_mf->sync(pgidStart, pgidEnd);
}

void
PageIOMem::scanLastPgId()
{
	// find last committed pg
	
	for(page_id_t pgid = m_mf->numPagesReserved() - 1; pgid != ~0UL; -- pgid)
	{
		Page pg(readPage(pgid));
		// std::cout << "scan pg " << pgid2str(pgid) << " commited: " << pg.isCommitted() << std::endl;
		if(pg.isCommitted())
		{
			m_pgidLast = pgid;
			return;
		}
	}

	m_pgidLast = PGID_INVALID;
}

page_id_t
PageIOMem::getLastPgId() const
{
	return m_pgidLast;
}

bool
PageIOMem::needInit() const
{
	return m_needInit;
}

void
PageIOMem::discardOldPages(page_id_t threshold)
{
	m_mf->unmap(threshold);
}

void
PageIOMem::dump(std::ostream& s) const
{
	s << "** PageIOMem stat dump **" << std::endl;
	s << "last alloced pgid: " << pgid2str(m_pgidLast) << std::endl;
	s << *m_mf;
}

void
PageIOMem::dumpStat() const
{
	std::cout << *this;
}

} // end of namespace ptnk
