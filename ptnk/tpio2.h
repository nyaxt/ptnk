#ifndef _ptnk_tpio2_h_
#define _ptnk_tpio2_h_

#include "pageio.h"
#include "stm.h"
#include "pol.h"

#include <boost/thread.hpp>

// Transactional PageIO impl. using stm.h

namespace ptnk
{

struct TPIOStat
{
	unsigned int nUniquePages;

	unsigned int nRead;
	unsigned int nReadOvr;
	unsigned int nReadOvrLocal;

	unsigned int nModifyPage;
	unsigned int nOvr;

	unsigned int nSync;

	unsigned int nNotifyOldLink;

	TPIOStat() :
		nRead(0),
		nReadOvr(0),
		nReadOvrLocal(0),
		nModifyPage(0),
		nOvr(0),
		nSync(0),
		nNotifyOldLink(0)
	{ /* NOP */ }

	void merge(const TPIOStat& o);
	void dump(std::ostream& o) const;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIOStat& o)
{ o.dump(s); return s; }

class TPIO2;

class TPIO2TxSession : public PageIO
{
public:
	~TPIO2TxSession();

	void dump(std::ostream& s) const;

	bool tryCommit();

	const TPIOStat& stat() const
	{
		return m_stat;	
	}

	// ====== implements PageIO interface ======
	pair<Page, page_id_t> newPage();

	Page readPage(page_id_t page); 
	Page modifyPage(const Page& page, mod_info_t* mod);
	void discardPage(page_id_t pgid, mod_info_t* mod);

	void sync(page_id_t pgid);

	page_id_t getLastPgId() const;

	void notifyPageWOldLink(page_id_t pgid);
	page_id_t updateLink(page_id_t pgidOld);

	// ====== start page accessor ======

	page_id_t pgidStartPage()
	{
		return m_lovr->pgidStartPage();	
	}

	void setPgidStartPage(page_id_t pgid)
	{
		m_lovr->setPgidStartPage(pgid);
	}

protected:
	friend class TPIO2; // give access to c-tor
	TPIO2TxSession(TPIO2* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr);

	PageIO* backend() const;

	PagesOldLink* oldlink()
	{
		return m_oldlink;
	};

	void addOvr(page_id_t pgidOrig, page_id_t pgidOvr)
	{
		m_lovr->addOvr(pgidOrig, pgidOvr);	
	}

	void loadStreak(BufferCRef bufStreak);

	struct OvrExtra : public LocalOvr::ExtraData
	{
		~OvrExtra();

		PagesOldLink oldlink;
	};

private:
	TPIO2* m_tpio;
	shared_ptr<ActiveOvr> m_aovr;
	unique_ptr<LocalOvr> m_lovr;
	PagesOldLink* m_oldlink;
	Vpage_id_t m_pagesModified;
	TPIOStat m_stat;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIO2TxSession& o)
{ o.dump(s); return s; }

constexpr unsigned int REBASE_THRESHOLD = TPIO_NHASH * 8;

class TPIO2
{
public:
	TPIO2(shared_ptr<PageIO> backend);

	~TPIO2();

	void dump(std::ostream& s) const;

	unique_ptr<TPIO2TxSession> newTransaction();
	bool tryCommit(TPIO2TxSession* tx);

	void rebase(bool force);
	void refreshOldPages(page_id_t threshold);

	PageIO* backend()
	{
		return m_backend.get();	
	}

	const TPIOStat& stat()
	{
		return m_stat;	
	}

private:
	class RebaseTPIO2TxSession : public TPIO2TxSession
	{
	public:
		RebaseTPIO2TxSession(TPIO2* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr, PagesOldLink* oldlink);
		~RebaseTPIO2TxSession();

		page_id_t updateLink(page_id_t idOld);
		page_id_t rebaseForceVisit(page_id_t pgid);

	private:
		page_id_t rebaseVisit(page_id_t pgid);

		Spage_id_t m_visited;
		const PagesOldLink* m_oldlinkRebase;
	};

	void syncDelayed(const Vpage_id_t& pagesModified);
	void commitTxPages(TPIO2TxSession* tx, ver_t verW, bool isRebase);

	void restoreState();

	shared_ptr<PageIO> m_backend;

	boost::shared_mutex m_mtxAOvr;
	shared_ptr<ActiveOvr> m_aovr;

	TPIOStat m_stat;

	//! true if rebase is being done (further tx are cancelled)
	bool m_bDuringRebase;

	boost::mutex m_mtxRebase;
	boost::condition_variable m_condRebase;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIO2& o)
{ o.dump(s); return s; }

inline
bool
TPIO2TxSession::tryCommit()
{
	return m_tpio->tryCommit(this);	
}

inline
PageIO*
TPIO2TxSession::backend() const
{
	return m_tpio->backend();
}

} // end of namespace ptnk

#endif // _ptnk_tpio2_h_
