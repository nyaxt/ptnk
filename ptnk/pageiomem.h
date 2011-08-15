#ifndef _ptnk_pageiomem_h_
#define _ptnk_pageiomem_h_

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread.hpp>

#include "pageio.h"

namespace ptnk
{

#if defined(__linux__) && defined(__x86_64__)
#define PTNK_MMAP_HINT (char*)0x600000000000
#else
// no hint
#define PTNK_MMAP_HINT NULL
#endif

// file sync strategies:
//   -t 100 -n 1000000 results:
//   ext2 @ sync_file_range	3.13 sec 
//   ext2 @ fdatasync		3.84 sec 
//   ext4 @ sync_file_range 36 sec
//   ext4 @ fdatasync		17 sec
#ifdef __linux__
#define PTNK_SYNC_FILE_RANGE
#endif
// #define PTNK_FDATASYNC

class PageIOMem : public PageIO
{
public:
	enum
	{
		NUM_PAGES_ALLOC_ONCE = 256,
	};

	PageIOMem(const char* filename = NULL, ptnk_opts_t opts = ODEFAULT, int mode = 0644);

	~PageIOMem();

	virtual pair<Page, page_id_t> newPage();

	virtual Page readPage(page_id_t page);
	virtual void sync(page_id_t pgid);
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	virtual page_id_t getLastPgId() const;

	virtual bool needInit() const;

	void dumpStat() const;

private:
	class Mapping
	{
	public:
		Mapping(char* offset, size_t numpages, page_id_t pgidStart, bool syncAtEnd)
		:	m_offset(offset), m_numpages(numpages), m_pgidStart(pgidStart), m_syncAtEnd(syncAtEnd)
		{
			m_pgidEnd = m_pgidStart + numpages;
		}

		~Mapping();

		//! unchecked ver. of calcPtr
		char* calcPtr2(page_id_t pgid)
		{
			return m_offset + PTNK_PAGE_SIZE * (pgid - m_pgidStart);
		}

		char* calcPtr(page_id_t pgid)
		{
			if(m_pgidStart <= pgid /* && pgid < m_pgidEnd */)
			{
				PTNK_ASSERT(pgid < m_pgidEnd) { dump(); }
				return calcPtr2(pgid);
			}
			else
			{
				return NULL;
			}
		}

		page_id_t pgidEnd()
		{
			return m_pgidEnd;	
		}

		page_id_t syncRange(page_id_t pgidStart, page_id_t pgidEnd);

		char* addrEnd()
		{
			return m_offset + m_numpages * PTNK_PAGE_SIZE;
		}

		void extend(size_t numpages)
		{
			m_numpages += numpages;
			m_pgidEnd += numpages;
		}

		void dump() const;

	private:
		char* m_offset;
		size_t m_numpages;
		page_id_t m_pgidStart;
		page_id_t m_pgidEnd;

		bool m_syncAtEnd;
	};

	char* calcPtr(page_id_t pgid);
	void addMapping(size_t numpages, Mapping* mapold = NULL, bool doExpand = true);

	bool m_needInit;

	int m_fd;
	bool m_isFile;

	volatile page_id_t m_idNext;
	boost::mutex m_mtxAllocNewMapping;

	size_t m_numPagesReserved;

	typedef boost::ptr_vector<Mapping> VPMapping;
	VPMapping m_maps;

	//! sync modified pages to file on sync() method
	bool m_sync;

	//! prot passed to mmap(2)
	int m_prot;
};

} // end of namespace ptnk

#endif // _ptnk_pageiomem_h_
