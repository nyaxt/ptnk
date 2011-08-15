#ifndef _ptnk_pageio_h_
#define _ptnk_pageio_h_

#include "page.h"
#include "buffer.h"
#include "types.h"
#include <boost/utility.hpp>

namespace ptnk
{

struct mod_info_t : boost::noncopyable
{
	//! the original read page's ovr tgt
	page_id_t idOrig;

	//! the new page created as ovr from mod
	/*!
	 *	@note
	 *		if idOvr == PGID_INVALID, it represents page discard
	 */
	page_id_t idOvr;

	void reset()
	{
		idOrig = idOvr = PGID_INVALID;
	}

	mod_info_t() { reset(); }

	bool isValid() { return idOrig != PGID_INVALID; }

	void dump() const;
};

class PageIO
{
public:
	virtual ~PageIO();

	//! alloc a page
	/*!
	 *	returned page's header is left uninitialized
	 *	callee must init its header explicitly
	 *
	 *	@sa newInitPage
	 */
	virtual pair<Page, page_id_t> newPage() = 0;

	//! alloc a page and init header of the page
	template<typename PAGE_T>
	PAGE_T 
	newInitPage()
	{
		pair<Page, page_id_t> np = newPage();
		PAGE_T ret(np.first, true /* ignore type check */); ret.init(np.second);
		return ret;	
	}

	//! request read access to a page
	virtual Page readPage(page_id_t id) = 0;

	//! request write access to previously read-access-acquired _page_
	/*!
	 *	The returned page handle may be OR MAY NOT BE the same page handle previously acquired.
	 *	Specifically, the ovr page implementation (TPIO::TxSession) may return new page
	 *	for place to write new content of the page.
	 *	The callee should NOT assume it to have the copy of the original content,
	 *	as the returned page WILL NOT have the original content when ovr page is created.
	 */
	virtual Page modifyPage(const Page& page, mod_info_t* mod);

	Page modifyPage(const Page& page, bool* bNewPage = NULL)
	{
		mod_info_t mod;
		Page ret(modifyPage(page, &mod));
		if(bNewPage && mod.isValid()) *bNewPage = true;
		return ret;
	}
	
	//! discard unused page _pgid_
	virtual void discardPage(page_id_t pgid, mod_info_t* mod = NULL);

	//! notify PageIO the page content update
	/*!
	 *	Actual disk sync may be delayed to time of commit depending on the implementation
	 *	(eg. TPIO::TxSession)
	 */
	virtual void sync(page_id_t pgid) = 0;
	
	void sync(const Page& pg)
	{
		sync(pg.pageId());	
	}

	//! notify PageIO content update for the pages where pgidStart <= pgid < pgidEnd
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	//! pgid of the youngest page accessible
	virtual page_id_t getFirstPgId() const;

	//! pgid of the page with max pgid
	virtual page_id_t getLastPgId() const = 0;

	//! get last valid localid of the partition _ptid_
	virtual local_pgid_t getPartLastLocalPgId(part_id_t ptid) const;

	virtual void notifyPageWOldLink(page_id_t pgid, page_id_t pgidDep = PGID_INVALID);
	virtual page_id_t updateLink(page_id_t pgidOld);

	//! check if an initial setup is needed
	/*!
	 *	return true if pages are uninitialized (new file), or
	 *	false if there is existing data which need to be read.
	 */
	virtual bool needInit() const;

	//! explicitly make new partition
	virtual void newPart(bool bForce = true);

	//! discard old pages for compaction
	/*!
	 *	@param [in] threshold
	 *		page w/ pgid < _threshold_ will be discarded
	 */
	virtual void discardOldPages(page_id_t threshold);

	virtual void dumpStat() const;
};

} // end of namespace ptnk

#endif // _ptnk_pageio_h_
