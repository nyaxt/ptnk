#include "mappedfile.h"
#include "sysutils.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace ptnk
{

MappedFile*
MappedFile::createNew(part_id_t partid, const std::string& filename, ptnk_opts_t opts, int mode)
{
	if(!(opts & OTRUNCATE) && file_exists(filename.c_str()))
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

		PTNK_ASSURE_SYSCALL(fd = ::open(filename.c_str(), flags, mode));
	}
	
	unique_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, PROT_READ | PROT_WRITE));
	mf->expandFile(NPAGES_PREALLOC);

	return mf.release();
}

MappedFile*
MappedFile::openExisting(part_id_t partid, const std::string& filename, ptnk_opts_t opts)
{
	// get file stat
	size_t filesize = 0;
	{
		struct stat st;
		PTNK_ASSURE_SYSCALL(::stat(filename.c_str(), &st) < 0);

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

		PTNK_ASSURE_SYSCALL(fd = ::open(filename.c_str(), flags));
	}

	unique_ptr<MappedFile> mf(new MappedFile(partid, filename, fd, prot));

	size_t pgs = filesize / PTNK_PAGE_SIZE;
	if(pgs > 0)
	{
		mf->moreMMap(pgs);
	}

	return mf.release();
}

MappedFile*
MappedFile::createMem()
{
	int fd;

	PTNK_ASSURE_SYSCALL(fd = ::open("/dev/zero", O_RDONLY));
	unique_ptr<MappedFile> ret(new MappedFile(0, "", fd, PROT_READ | PROT_WRITE));
	ret->expandFile(NPAGES_PREALLOC);

	return ret.release();
}

MappedFile::MappedFile(part_id_t partid, const std::string& filename, int fd, int prot)
:	m_partid(partid), m_fd(fd), m_prot(prot), m_numPagesReserved(0)
{
	if(! filename.empty())
	{
		m_filename = filename;
		m_bInMem = false;
	}
	else
	{
		m_bInMem = true;
	}

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

	if(! m_bInMem)
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
	if(! m_bInMem)
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
	if(m_bInMem) return; // no need to sync when not mapped to file

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


} // end of namespace ptnk
