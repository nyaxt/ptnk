#ifndef _ptnk_pageiomem_h_
#define _ptnk_pageiomem_h_

#include "pageio.h"

#include <thread>

namespace ptnk
{

class MappedFile;

class PageIOMem : public PageIO
{
public:
	PageIOMem(const char* filename = NULL, ptnk_opts_t opts = ODEFAULT, int mode = 0644);

	~PageIOMem();

	virtual pair<Page, page_id_t> newPage();

	virtual Page readPage(page_id_t pgid);
	virtual void sync(page_id_t pgid);
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	virtual page_id_t getLastPgId() const;

	virtual bool needInit() const;

	virtual void discardOldPages(page_id_t threshold);

	void dump(std::ostream& s) const;
	void dumpStat() const;

private:
	//! scan last committed pg (used when opening existing file)
	void scanLastPgId();

	//! alloc more pages
	void expandTo(page_id_t pgid);

	bool m_needInit;

	bool m_isFile;

	//! last alloc-ed pgid
	volatile page_id_t m_pgidLast;

	//! sync modified pages to file on sync() method
	bool m_sync;

	std::unique_ptr<MappedFile> m_mf;

	//! mtx for m_mf->expandFile
	std::mutex m_mtxAlloc;
};
inline
std::ostream& operator<<(std::ostream& s, const PageIOMem& o)
{ o.dump(s); return s; }
} // end of namespace ptnk

#endif // _ptnk_pageiomem_h_
