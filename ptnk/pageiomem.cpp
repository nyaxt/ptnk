#include "pageiomem.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <boost/foreach.hpp>
#include <boost/tuple/tuple.hpp>

namespace ptnk
{

// #define VERBOSE_PAGEIOMEM

PageIOMem::PageIOMem(const char* filename, ptnk_opts_t opts, int mode)
:	m_fd(-1), m_idNext(0), m_numPagesReserved(0),
	m_sync(opts & OAUTOSYNC), // FIXME
	m_prot(0)
{
#ifdef VERBOSE_PAGEIOMEM
	std::cout << "new PageIOMem" << std::endl;
#endif

	if(filename == NULL || *filename == '\0')
	{
		m_isFile = false;
#ifdef __APPLE__
		m_fd = -1;
#else
		m_fd = ::open("/dev/zero", O_RDONLY);
#endif
		m_needInit = true;
	}
	else
	{
		m_isFile = true;

		// get file stat
		bool doesExist = false;
		size_t filesize = 0;
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
				filesize = st.st_size;
			}
		}
#ifdef VERBOSE_PAGEIOMEM
		std::cout << "PageIOMem filename:" << filename << " doesExist: " << doesExist << " filesize: " << filesize << std::endl;
#endif

		// open file
		{
			int flags = 0;
			m_prot = PROT_READ;

			if(opts & OWRITER)
			{
				flags |= O_RDWR;
				m_prot |= PROT_WRITE;
			}
			else
			{
				flags |= O_RDONLY;
			}

			if(opts & OCREATE)
			{
				flags |= O_CREAT;
				if(opts & OTRUNCATE) flags |= O_TRUNC;
			}

			PTNK_ASSURE_SYSCALL(m_fd = ::open(filename, flags, mode));
		}

		if(doesExist && filesize > 0 && !(opts & OTRUNCATE))
		{
			addMapping(filesize / PTNK_PAGE_SIZE, NULL, false);

			// find last committed pg
			for(page_id_t pgid = m_numPagesReserved - 1; pgid >= 0; -- pgid)
			{
				Page pg(readPage(pgid));
				if(pg.isCommitted())
				{
					m_idNext = pgid + 1;
					break;
				}
			}
#ifdef VERBOSE_PAGEIOMEM
			std::cout << "PageIOMem last valid pgid:" << m_idNext - 1 << std::endl;
#endif

			m_needInit = false;	
		}
		else
		{
			m_needInit = true;	
		}
	}

	if(m_needInit)
	{
		addMapping(1024);
	}

#ifdef VERBOSE_PAGEIOMEM
	dumpStat();
#endif
}

PageIOMem::~PageIOMem()
{
	if(m_fd > 0)
	{
		::close(m_fd);
	}
}

void
PageIOMem::addMapping(size_t numpages, Mapping* mapold, bool doExpand)
{
#ifdef VERBOSE_PAGEIOMEM
	std::cout << "PageIOMem alloc " << numpages << " pages" << std::endl;
#endif

	char *offset, *hint = mapold ? mapold->addrEnd() : PTNK_MMAP_HINT;
	if(m_isFile)
	{
		if(doExpand)
		{
			size_t allocsize = numpages * PTNK_PAGE_SIZE;
		#ifdef USE_POSIX_FALLOCATE
			PTNK_ASSURE_SYSCALL(::posix_fallocate(m_fd, m_numPagesReserved * PTNK_PAGE_SIZE, allocsize));
		#else
			// expand file first
			boost::scoped_ptr<char> buf(new char[allocsize + PTNK_PAGE_SIZE]);
			PTNK_ASSURE_SYSCALL(::pwrite(m_fd, buf.get(), allocsize, m_numPagesReserved * PTNK_PAGE_SIZE));
			PTNK_ASSURE_SYSCALL(::fsync(m_fd));
		#endif
		}

		PTNK_ASSURE_SYSCALL_NEQ(
			offset = static_cast<char*>(::mmap(hint, numpages * PTNK_PAGE_SIZE, m_prot, MAP_SHARED, m_fd, m_numPagesReserved * PTNK_PAGE_SIZE)),
			MAP_FAILED
			)
		{
			std::cerr << "m_fd: " << m_fd<< std::endl;	
		};
	}
	else
	{
		PTNK_ASSURE_SYSCALL_NEQ(
			offset = static_cast<char*>(::mmap(hint, numpages * PTNK_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, m_fd, 0)),
			MAP_FAILED
			)
		{
			std::cerr << "m_fd: " << m_fd<< std::endl;	
		};
	}
	if(mapold && offset == hint)
	{
		// extend existing map
		mapold->extend(numpages);
	}
	else
	{
		Mapping* map = new Mapping(offset, numpages, (page_id_t)m_numPagesReserved,
	#ifdef PTNK_SYNC_FILE_RANGE
			// the valid pages should have been synced by sync_file_range
			//   what actually happens at msync is sync of unused but alloced pages, which is good for nothing
			m_isFile && !m_sync
	#else
			m_isFile
	#endif
			);
		m_maps.push_back(map);
	}
	m_numPagesReserved += numpages;

#ifdef VERBOSE_PAGEIOMEM
	std::cout << "currently " << m_numPagesReserved << " pages reserved" << std::endl;
#endif
}

inline
char*
PageIOMem::calcPtr(page_id_t pgid)
{
	char* ret = NULL;

	VPMapping::reverse_iterator itMap = m_maps.rbegin();
	PTNK_ASSERT(pgid < itMap->pgidEnd());
	for(; itMap != m_maps.rend(); ++ itMap)
	{
		ret = itMap->calcPtr(pgid);
		if(ret != NULL) return ret;
	}

	PTNK_ASSERT(false) { std::cout << "should not come here" << std::endl; }
	return ret;
}

pair<Page, page_id_t>
PageIOMem::newPage()
{
	page_id_t idResv;

RETRY:
	idResv = m_idNext;
	if(idResv >= m_numPagesReserved)
	{
		// need more pages...
#ifdef VERBOSE_PAGEIOMEM
		std::cout << "running out of space" << std::endl;
#endif
		boost::lock_guard<boost::mutex> g(m_mtxAllocNewMapping);

		// make sure that other thread has not already alloced pages
		ssize_t numNeeded = m_idNext - m_numPagesReserved + 1;
		if(numNeeded > 0)
		{
#ifdef VERBOSE_PAGEIOMEM
			std::cout << "I'm the one going to alloc!!!" << std::endl;
#endif

			// do alloc
			size_t numalloc = std::max(numNeeded, (ssize_t)NUM_PAGES_ALLOC_ONCE);
			addMapping(numalloc, &m_maps.back());
		}
		
		goto RETRY;
	}

	if(! __sync_bool_compare_and_swap(&m_idNext, idResv, idResv+1))
	{
		// reservation failed

		goto RETRY;
	}

	return make_pair(Page(calcPtr(idResv), true), idResv);
}

Page
PageIOMem::readPage(page_id_t id)
{
	PTNK_ASSERT(id != PGID_INVALID);
	return Page(calcPtr(id), false);
}

void
PageIOMem::sync(page_id_t pgid)
{
	// ::memcpy(m_buf + PTNK_PAGE_SIZE * page.pageId(), page.get(), PTNK_PAGE_SIZE);
	if(m_sync && m_isFile)
	{
		char* pgoffset = calcPtr(pgid);
		PTNK_ASSURE_SYSCALL(::msync(pgoffset, PTNK_PAGE_SIZE, MS_SYNC | MS_INVALIDATE));
	}
}

void
PageIOMem::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	// std::cout << "syncRange " << pgid2str(pgidStart) << " to " << pgid2str(pgidEnd) << std::endl;

	if(!m_sync || !m_isFile) return;

#ifdef PTNK_FDATASYNC
	PTNK_ASSURE_SYSCALL(::fdatasync(m_fd));
	return;
#endif

	VPMapping::reverse_iterator itMap = m_maps.rbegin();

	page_id_t idLast = itMap->pgidEnd();
	if(pgidEnd >= idLast)
	{
		pgidEnd = idLast - 1;
	}

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
	for(; itMap != m_maps.rend(); ++ itMap)
	{
		pgidEnd = itMap->syncRange(pgidStart, pgidEnd);
		if(pgidEnd == PGID_INVALID) break;
	}
#endif
}

page_id_t
PageIOMem::getLastPgId() const
{
	return m_idNext - 1;
}

bool
PageIOMem::needInit() const
{
	return m_needInit;	
}

void
PageIOMem::dumpStat() const
{
	std::cout << "** PageIOMem stat dump **" << std::endl;
	std::cout << "num of current pages: " << m_idNext << std::endl;
	std::cout << "num of reserved pages: " << m_numPagesReserved << std::endl;
	BOOST_FOREACH(const Mapping& map, m_maps)
	{
		map.dump();	
	}
}

PageIOMem::Mapping::~Mapping()
{
	if(m_syncAtEnd)
	{
		PTNK_ASSURE_SYSCALL(::msync(m_offset, m_numpages * PTNK_PAGE_SIZE, MS_SYNC | MS_INVALIDATE));
	}
	::munmap(m_offset, m_numpages * PTNK_PAGE_SIZE);
}

inline
page_id_t
PageIOMem::Mapping::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	PTNK_ASSERT(pgidEnd < m_pgidEnd);
	// std::cout << "syncRange: " << pgidStart << " to " << pgidEnd << std::endl;

	page_id_t ret;
	if(pgidStart < m_pgidStart)	
	{
		ret = m_pgidStart - 1;	
		pgidStart = m_pgidStart;
	}
	else
	{
		ret = PGID_INVALID;	
	}

	{
		char* start = calcPtr2(pgidStart);
		size_t size = (pgidEnd - pgidStart + 1) * PTNK_PAGE_SIZE;
		PTNK_ASSURE_SYSCALL(::msync(start, size, MS_SYNC | MS_INVALIDATE));
	}

	return ret;
}

void
PageIOMem::Mapping::dump() const
{
	std::cout << "  map [" << m_pgidStart << ", " << m_pgidEnd << ") " << (void*)m_offset << std::endl;
}

} // end of namespace ptnk
