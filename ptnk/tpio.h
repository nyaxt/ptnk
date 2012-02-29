#ifndef _ptnk_tpio_h_
#define _ptnk_tpio_h_

#include "pageio.h"
#include "stm.h"
#include "pol.h"

#include <thread>

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
		nUniquePages(0),
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

class TPIO;

class TPIOTxSession : public PageIO
{
public:
	~TPIOTxSession();

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

	void discardOldPages(page_id_t threshold);

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
	friend class TPIO; // give access to c-tor
	TPIOTxSession(TPIO* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr);

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
	TPIO* m_tpio;
	shared_ptr<ActiveOvr> m_aovr;
	unique_ptr<LocalOvr> m_lovr;
	PagesOldLink* m_oldlink;
	Vpage_id_t m_pagesModified;
	TPIOStat m_stat;

	// for TPIO::TxPool
public:
	void embedRegIdx(size_t regtxidx)
	{
		m_regtxidx = regtxidx;	
	}

	size_t regIdx() const
	{
		return m_regtxidx;	
	}

private:
	size_t m_regtxidx;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIOTxSession& o)
{ o.dump(s); return s; }

constexpr unsigned int REBASE_THRESHOLD = TPIO_NHASH * 8;
constexpr size_t REFRESH_PGS_PER_TX_DEFAULT = 128;

class TPIO
{
public:
	TPIO(shared_ptr<PageIO> backend, ptnk_opts_t opts = OAUTOSYNC);

	~TPIO();

	void dump(std::ostream& s) const;

	unique_ptr<TPIOTxSession> newTransaction();

	bool tryCommit(TPIOTxSession* tx, commit_flags_t flags = COMMIT_DEFAULT);

	void rebase(bool force);
	void refreshOldPages(page_id_t threshold, size_t pgsPerTx = REFRESH_PGS_PER_TX_DEFAULT);
	void join();

	PageIO* backend()
	{
		return m_backend.get();	
	}

	const TPIOStat& stat()
	{
		return m_stat;	
	}

private:
	class RebaseTPIOTxSession : public TPIOTxSession
	{
	public:
		RebaseTPIOTxSession(TPIO* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr, PagesOldLink* oldlink);
		~RebaseTPIOTxSession();

		page_id_t updateLink(page_id_t idOld);
		page_id_t rebaseForceVisit(page_id_t pgid);

	private:
		page_id_t rebaseVisit(page_id_t pgid);

		Spage_id_t m_visited;
		const PagesOldLink* m_oldlinkRebase;
	};

	void syncDelayed(const Vpage_id_t& pagesModified);
	void commitTxPages(TPIOTxSession* tx, ver_t verW, bool isRebase);

	void restoreState();

	shared_ptr<PageIO> m_backend;
	bool m_sync;

	std::mutex m_mtxAOvr;
	shared_ptr<ActiveOvr> m_aovr;

	TPIOStat m_stat;

	//! true if rebase is being done (further tx are cancelled)
	bool m_bDuringRebase;

	//! true if refresh is being done
	bool m_bDuringRefresh;

	std::mutex m_mtxRebase;
	std::condition_variable m_condRebase;

	class TxPool;
	unique_ptr<TxPool> m_txpool;
public:
	void registerTx(TPIOTxSession* tx);
	void unregisterTx(TPIOTxSession* tx);
};
inline
std::ostream& operator<<(std::ostream& s, const TPIO& o)
{ o.dump(s); return s; }

inline
bool
TPIOTxSession::tryCommit()
{
	return m_tpio->tryCommit(this);	
}

inline
PageIO*
TPIOTxSession::backend() const
{
	return m_tpio->backend();
}

} // end of namespace ptnk

#endif // _ptnk_tpio_h_
