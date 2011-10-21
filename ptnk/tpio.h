#ifndef _ptnk_tpio_h_
#define _ptnk_tpio_h_

#include "pageio.h"
#include "pol.h"

#include <boost/shared_ptr.hpp>
#include <boost/array.hpp>
#include <boost/thread.hpp>

namespace ptnk
{

//! vector containing override info (used in each transaction attempt)
class OverridesV
{
public:
	OverridesV();

	~OverridesV();

	void add(page_id_t orig, page_id_t ovr);

	page_id_t find(page_id_t orig) const;

	void dump() const;

	// for checkConflict / merge
	friend class OverridesCB;

private:
	struct entry_t
	{
		page_id_t orig;
		page_id_t ovr;

		struct orig_eq_p
		{
			page_id_t tgt;
			orig_eq_p(page_id_t tgt_) :	tgt(tgt_) { /* NOP */ }

			inline bool operator()(const entry_t& e) const { return e.orig == tgt; };
		};
	};

	typedef std::vector<entry_t> v_t;
	v_t m_v;
};

//! circular buffer containing override info. (kept in TPIO)
class OverridesCB
{
public:
	enum {
		BUF_SIZE = 32 * 1024,
		HASH_BITS = 12,
		HASH_RANGE = 1 << (HASH_BITS-1),
	};

	OverridesCB();

	~OverridesCB();

	typedef int hint_t;
	page_id_t find(page_id_t orig, tx_id_t verBase = (page_id_t)(~0), tx_id_t ver = (page_id_t)(~0), hint_t hint = -1) const;

	//! check for conflict
	/*!
	 *	@return
	 *		true if no conflict detected
	 */
	bool checkConflict(const OverridesV& v, tx_id_t verBase, tx_id_t verSS, hint_t hint = -1) const;

	bool filterConflict(OverridesV& v, tx_id_t verBase, tx_id_t verSS, hint_t hint = -1) const;

	//! merge overrides
	void merge(const OverridesV& v, tx_id_t verBase, tx_id_t verTx);

	//! add entry (add via OverridesV instead where possible)
	void add(page_id_t orig, page_id_t ovr, tx_id_t ver);

	hint_t getHint();

	ssize_t getSpaceLeft(tx_id_t verBase, hint_t hint);

	void dump() const;
	void dumpStat() const;

	char* niseDumpState(char* buf);
	const char* niseRestoreState(const char* buf);

	struct entry_t
	{
		page_id_t orig;
		page_id_t ovr;
		tx_id_t ver;
		
		//! previous entry with same hash
		int idxPrevHash;
	};
	
private:
	static unsigned int schash(page_id_t pgid);

	boost::array<entry_t, BUF_SIZE> m_cb;
	int m_idxNext;

	boost::array<int, HASH_RANGE> m_shortcut;

	tx_id_t m_verDamaged;
};

class OverridesCache
{
public:
	enum
	{
		NUM_CACHE_ENTRY = 256,
	};

	OverridesCache();

	void add(page_id_t pgidOrig, page_id_t pgidOvr);
	pair<bool, page_id_t> lookup(page_id_t pgidOrig);

private:
	struct cache_entry_t
	{
		page_id_t pgidOrig;	
		page_id_t pgidOvr;	
	};
	typedef std::vector<cache_entry_t> Vcache_entry_t;

	Vcache_entry_t m_impl;
};

#define TXSESSION_BATCH_SYNC

struct SnapshotState
{
	//! snapshot base version
	/*!
	 *	version of the root node in the snapshot version
	 *	This will be used to check if the ovrs from the base version are still accessible,
	 *	if they are not, tx will be aborted.
	 */
	tx_id_t verBase;

	//! snapshot version
	/*!
	 *	changes after this (pages after this ver) will not be seen by this transaction.
	 *	This ensures that it will not read changes made by other tx
	 *	(read only committed pages at that point = snapshot isolation)
	 */
	tx_id_t ver;

	OverridesCB::hint_t ovrshint;

	page_id_t pgidStartPage;
};

class TPIO;

class TPIOTxSession : public PageIO
{
public:
	~TPIOTxSession();

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
		return m_state.pgidStartPage;	
	}

	void setPgidStartPage(page_id_t pgid)
	{
		m_state.pgidStartPage = pgid;	
	}

	bool tryCommit();

	struct Stat
	{
		unsigned int nRead;
		unsigned int nReadOvr;
		unsigned int nReadOvrLocal;

		unsigned int nModifyPage;
		unsigned int nNewOvr;

		unsigned int nSync;

		unsigned int nNotifyOldLink;

		unsigned int nCommitFail;

		Stat() :
			nRead(0),
			nReadOvr(0),
			nReadOvrLocal(0),
			nModifyPage(0),
			nNewOvr(0),
			nSync(0),
			nNotifyOldLink(0),
			nCommitFail(0)
		{ /* NOP */ }
		void dump() const;
	};
	const Stat& stat() const
	{
		return m_stat;	
	}

	void dumpStat() const
	{
		m_stat.dump();	
	}

protected:
	enum session_type_t
	{
		SESSION_NORMAL,
		SESSION_REBASE, //!< RebaseTPIOTxSession
		SESSION_REFRESH, //!< RefreshTPIOTxSession
	};

	friend class TPIO; // give access to c-tor
	TPIOTxSession(TPIO* tpio, session_type_t type = SESSION_NORMAL);

	void syncDelayed();

	void fillTPIOHeaders();

	TPIO* m_tpio;

	//! m_tpio->m_backend cached
	PageIO* m_backend;

	//! id of this transaction (not set until commit is done)
	tx_id_t m_txid;

	SnapshotState m_state;

	//! tx local overrides
	OverridesV m_ovrsTxLocal;

	PagesOldLink m_oldlinkLocal;

	//! set of locally modified / created pages
	Spage_id_t m_pagesModified;

	Stat m_stat;

	session_type_t m_type;

	OverridesCache m_cache;

	uint64_t m_numUniquePagesLocal;
};

//! transactional page io
class TPIO
{
public:
	enum
	{
		REBASE_THRESHOLD = 1024,	
	};

	TPIO(boost::shared_ptr<PageIO> backend);

	~TPIO();

	friend class TPIOTxSession;

	TPIOTxSession* newTransaction();
	
	void rebase(bool force);
	void refreshOldPages(page_id_t threshold);

	void dumpStat() const;

	char* niseDumpState(char* buf);
	const char* niseRestoreState(const char* buf);

	unsigned int numUniquePages() const
	{
		return m_numUniquePages;
	}

private:
	class RebaseTPIOTxSession : public TPIOTxSession
	{
	public:
		RebaseTPIOTxSession(TPIO* tpio)
		: TPIOTxSession(tpio, TPIOTxSession::SESSION_REBASE)
		{ /* NOP */ }

		~RebaseTPIOTxSession()
		{ /* NOP */ }

		page_id_t updateLink(page_id_t idOld);
		page_id_t rebaseForceVisit(page_id_t pgid);

	private:
		page_id_t rebaseVisit(page_id_t pgid);

		Spage_id_t m_visited;
	};
	friend class TxRebaseSession;

	class RefreshTPIOTxSession : public TPIOTxSession
	{
	public:
		RefreshTPIOTxSession(TPIO* tpio)
		: TPIOTxSession(tpio, TPIOTxSession::SESSION_REFRESH)
		{ /* NOP */ }

		~RefreshTPIOTxSession()
		{ /* NOP */ }
	};
	friend class RefreshTPIOTxSession;

	void handleStreak(BufferCRef bufStreak);
	void restoreState();

	boost::mutex m_mtx;

	boost::shared_ptr<PageIO> m_backend;
	OverridesCB m_ovrs;

	PagesOldLink m_oldlink;

	boost::shared_mutex m_mtxState;
	SnapshotState m_stateTip;

	uint64_t m_numUniquePages;
};

} // end of namespace ptnk

#endif // _ptnk_tpio_h_
