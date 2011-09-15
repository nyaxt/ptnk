#include "tpio.h"
#include "sysutils.h"
#include "hash.h"
#include "streak.h"

#include <boost/tuple/tuple.hpp> // boost::tie

namespace ptnk
{

OverridesV::OverridesV()
{
	/* NOP */
}

OverridesV::~OverridesV()
{
	/* NOP */
}

void
OverridesV::add(page_id_t orig, page_id_t ovr)
{
	PTNK_ASSERT(orig != ovr);

	// overwrite existing entry if found
	v_t::iterator it;
	for(it = m_v.begin(); it != m_v.end(); ++ it)
	{
		if(it->orig == orig)
		{
			it->ovr = ovr;
			return;
		}
	}

	// add new entry
	entry_t e = {orig, ovr};
	m_v.push_back(e);
}

page_id_t 
OverridesV::find(page_id_t orig) const
{
	// find override entry
	v_t::const_reverse_iterator it
		= std::find_if(m_v.rbegin(), m_v.rend(), entry_t::orig_eq_p(orig));
	if(it != m_v.rend())
	{
		return it->ovr;
	}

	return orig;
}

void
OverridesV::dump() const
{
	std::cout << "ovrv dump:" << std::endl;
	BOOST_FOREACH(const entry_t& e, m_v)
	{
		std::cout << "* e orig: " << e.orig << " -> ovr: " << e.ovr << std::endl;
	}
}

OverridesCB::OverridesCB()
:	m_idxNext(0),
	m_verDamaged(0) // FIXME: verDamaged should be verLoaded - 1
{
	// clear circular buffer content
	BOOST_FOREACH(entry_t& e, m_cb)
	{
		e.orig = PGID_INVALID;
		e.ovr = PGID_INVALID;
		e.ver = m_verDamaged;
	}

	BOOST_FOREACH(int& e, m_shortcut)
	{
		e = -1;
	}
}

OverridesCB::~OverridesCB()
{
	/* NOP */
}

inline
void
OverridesCB::add(page_id_t orig, page_id_t ovr, tx_id_t ver)
{
	// add new entry
	{
		entry_t& eOvr = m_cb[m_idxNext]; // entry to be overwritten
		eOvr.orig = orig;
		eOvr.ovr = ovr;
		eOvr.ver = ver;

		unsigned int hash = schash(orig);
		int& sce = m_shortcut[hash];
		eOvr.idxPrevHash = sce;
		__sync_synchronize();
		sce = m_idxNext;
	}

	// inc ring idx
	++ m_idxNext;
	if(m_idxNext == (ssize_t)m_cb.size())
	{
		// act as ring buf
		m_idxNext = 0;
	}

	// entry at m_idxNext is invalidated
	{
		entry_t& eInv = m_cb[m_idxNext]; // entry to be invalidated
		PTNK_ASSERT(eInv.ver >= m_verDamaged);
		if(eInv.ver > m_verDamaged)
		{
			m_verDamaged = eInv.ver;
		}
	}
}

#define OverridesCB_forall_m_cb(I, BLK) \
	if(m_idxNext < I) { for(; I < (ssize_t)m_cb.size(); ++ I) BLK I = 0; }\
	for(; I < m_idxNext; ++ I) BLK

#define OverridesCB_forallr_m_cb(I, IE, BLK) \
	if(m_idxNext < IE) { \
		for(I = m_idxNext - 1; I >= 0; -- I) BLK \
		for(I = (ssize_t)m_cb.size()-1; I >= IE; -- I) BLK \
	} else { \
		for(I = m_idxNext - 1; I >= IE; -- I) BLK \
	}

page_id_t
OverridesCB::find(page_id_t orig, tx_id_t verBase, tx_id_t ver, hint_t hint) const
{
	if(verBase <= m_verDamaged)
	{
		PTNK_THROW_RUNTIME_ERR("ovr data for specified gen already overwritten");
	}

#if 0
	page_id_t ret1 = orig;

	int i1;
	int i, iE = m_idxNext+1;
	OverridesCB_forallr_m_cb(i, iE, {
		const entry_t& e = m_cb[i];

		if(e.ver < verBase) goto END;

		if(e.orig == orig)
		{
			if(e.ver > ver) continue;
			ret1 = e.ovr;
			i1 = i;
			goto END;
		}
	})
END:;
return ret1;
#endif

	page_id_t ret2 = orig;
	{
		unsigned int hash = schash(orig);
		int i = m_shortcut[hash];
		bool looped = false;
		for(;;)
		{
			if(i < 0) break;

			// std::cout << "ocb::find checking: " << i << std::endl;
			const entry_t& e = m_cb[i];
			if(e.ver < verBase) break;

			if(e.orig == orig && e.ver <= ver)
			{
				ret2 = e.ovr;
				break;
			}

			if(schash(e.orig) != hash) break;

			// avoid inf loop
			if(i == e.idxPrevHash) break;
			if(i < e.idxPrevHash)
			{
				if(looped) break;
				looped = true;	
			}

			i = e.idxPrevHash;
		}
	}

#if 0
	PTNK_ASSERT(ret1 == ret2)
	{
		std::cout << "ret1: " << pgid2str(ret1) << std::endl;
		std::cout << "ret2: " << pgid2str(ret2) << std::endl;
	}
#endif

	return ret2;
}

bool
OverridesCB::checkConflict(const OverridesV& v, tx_id_t verBase, tx_id_t verSS, hint_t hint) const
{
	// FIXME: nested loop conflict check -> inefficient...
	if(verBase <= m_verDamaged)
	{
		// ovr data for specified gen already overwritten => uncheckable
		return false;
	}

	int i, iE = m_idxNext+1;
	OverridesCB_forallr_m_cb(i, iE, {
		const entry_t& eCB = m_cb[i];

		if(eCB.ver <= verSS) break;

		BOOST_FOREACH(const OverridesV::entry_t& e, v.m_v)
		{
			// detect collision!
			if(eCB.orig == e.orig) return false;
		}
	})

	return true;
}

bool
OverridesCB::filterConflict(OverridesV& v, tx_id_t verBase, tx_id_t verSS, hint_t hint) const
{
	// FIXME: nested loop conflict check -> inefficient...
	if(verBase <= m_verDamaged)
	{
		// ovr data for specified gen already overwritten => uncheckable
		return false;
	}

	const int ivE = v.m_v.size();
	std::vector<int> mask(1, ivE);

	int i, iE = m_idxNext+1;
	OverridesCB_forallr_m_cb(i, iE, {
		const entry_t& eCB = m_cb[i];

		if(eCB.ver <= verSS) break;

		for(int iv = 0; iv < ivE; ++ iv)
		{
			const OverridesV::entry_t& e = v.m_v[iv];

			// detect collision!
			if(eCB.orig == e.orig)
			{
				mask[iv] = 0;
			}
		}
	})

	OverridesV::v_t newv; newv.reserve(ivE);
	for(int iv = 0; iv < ivE; ++ iv)
	{
		if(mask[iv])
		{
			newv.push_back(v.m_v[iv]);
		}
	}
	v.m_v.swap(newv);
	return true;
}

void
OverridesCB::merge(const OverridesV& v, tx_id_t verBase, tx_id_t verSS, tx_id_t verTx)
{
	// PTNK_ASSERT(checkConflict(v, verBase, verSS));

	BOOST_FOREACH(const OverridesV::entry_t& e, v.m_v)
	{
		if(e.ovr == PGID_INVALID) continue;

		add(e.orig, e.ovr, verTx);
	}
}

OverridesCB::hint_t
OverridesCB::getHint()
{
	return m_idxNext;
}

ssize_t
OverridesCB::getSpaceLeft(tx_id_t verBase, hint_t hint)
{
	if(verBase <= m_verDamaged)
	{
		return -1;	
	}
	else
	{
		ssize_t ret = m_cb.size() - 1;

		if(m_idxNext < hint)
		{
			ret -= m_cb.size() - hint;
			hint = 0;
		}
		ret -= m_idxNext - hint;

		return ret;
	}
}

void
OverridesCB::dump() const
{
	std::cout << "overridescb dump:" << std::endl;
	ssize_t i = m_idxNext;
	OverridesCB_forall_m_cb(i, {
		const entry_t& e = m_cb[i];
		std::cout << "* e orig: " << e.orig << " -> ovr: " << e.ovr << " ver: " << e.ver << std::endl;
	})
}

void
OverridesCB::dumpStat() const
{
	// FIXME: ???
}

inline
unsigned int
OverridesCB::schash(page_id_t pgid)
{
	return static_cast<unsigned int>(hashs(pgid, 0)) & (HASH_RANGE-1);
}

PagesOldLink::PagesOldLink()
{
	/* NOP */
}

PagesOldLink::~PagesOldLink()
{
	/* NOP */
}

// #define DUMP_POL_UPD

void
PagesOldLink::merge(const PagesOldLink& o)
{
	// add entries in o
	std::copy(o.m_impl.begin(), o.m_impl.end(), std::inserter(m_impl, m_impl.begin()));

#ifdef DUMP_POL_UPD
	std::cout << "dump upd" << std::endl;
	BOOST_FOREACH(const entry_t& e, m_impl)
	{
		std::cout << "e pgid: " << e.pgid << " dep: " << e.pgidDep << std::endl;
	}
#endif // DUMP_POL_UPD
}

OverridesCache::OverridesCache()
:	m_impl(NUM_CACHE_ENTRY)
{
	BOOST_FOREACH(cache_entry_t& e, m_impl)
	{
		e.pgidOrig = PGID_INVALID;	
	}
}

void
OverridesCache::add(page_id_t pgidOrig, page_id_t pgidOvr)
{
	cache_entry_t& e = m_impl[pgidOrig % NUM_CACHE_ENTRY];
	e.pgidOrig = pgidOrig;
	e.pgidOvr = pgidOvr;
}

pair<bool, page_id_t>
OverridesCache::lookup(page_id_t pgidOrig)
{
	const cache_entry_t& e = m_impl[pgidOrig % NUM_CACHE_ENTRY];
	if(e.pgidOrig == pgidOrig)
	{
		return make_pair(true, e.pgidOvr);	
	}
	else
	{
		return make_pair(false, pgidOrig);
	}
}

TPIO::TPIO(boost::shared_ptr<PageIO> backend)
:	m_backend(backend)
{
	m_stateTip.ovrshint = m_ovrs.getHint();

	if(m_backend->needInit())
	{
		m_stateTip.ver = 1;
		m_stateTip.verBase = 1;
		m_stateTip.pgidStartPage = PGID_INVALID;

		m_numUniquePages = 0;
	}
	else
	{
		restoreState();	
	}
}

struct PageVer
{
	page_id_t pgid;
	tx_id_t ver;

	inline bool operator<(const PageVer& o) const
	{
		if(ver != o.ver)
		{
			return ver > o.ver;
		}
		else
		{
			return pgid > o.pgid;	
		}
	}
};
typedef std::vector<PageVer> VPageVer;

void
TPIO::handleStreak(BufferCRef bufStreak)
{
	if(bufStreak.empty()) return;
	
	bufStreak.popFrontTo(&m_numUniquePages, sizeof(uint64_t));
	m_oldlink.restore(bufStreak);
}

#define PTNK_BKWD_SCAN(pio) \
	page_id_t pgid = pio->getLastPgId(); \
	for(part_id_t partid = PGID_PARTID(pgid); partid != PTNK_PARTID_INVALID; -- partid) \
	for(pgid = PGID_PARTLOCAL(partid, pio->getPartLastLocalPgId(partid)); PGID_LOCALID(pgid) != PGID_LOCALID_MASK; -- pgid)

void
TPIO::restoreState()
{
	// find last rebase tx and restore ovrs table	

	// 1. scan the log backwards until rebase tx is found
	// remember the pages found in the process
	VPageVer pagevers;
	m_stateTip.ver = 1;
	m_stateTip.verBase = 1;
	m_stateTip.pgidStartPage = PGID_INVALID;
	PTNK_BKWD_SCAN(m_backend)
	{
		// std::cerr << "restorestate scan: " << pgid2str(pgid) << " partid: " << partid << std::endl;

		Page pg(m_backend->readPage(pgid));
		page_hdr_t::flags_t flags = pg.hdr()->flags;
		tx_id_t ver = pg.hdr()->txid;

		// skip invalid page
		if(! pg.isCommitted()) continue;

		// find start page
		if(m_stateTip.pgidStartPage == PGID_INVALID && pg.pageType() == PT_DB_OVERVIEW)
		{
			m_stateTip.pgidStartPage = pgid;
		}

		// update ver
		if(flags & page_hdr_t::PF_END_TX)
		{
			if(ver > m_stateTip.ver)
			{
				m_stateTip.ver = ver;
			}

			if(flags & page_hdr_t::PF_TX_REBASE)
			{
				// rebase tx found. exit loop
				m_stateTip.verBase = ver;

				// FIXME: This code assumes that no concurrent tx cross rebase tx pages
				//        However, this may not be the case in future.
			}
		}
		
		if(ver >= m_stateTip.verBase)
		{
			pagevers.push_back((PageVer){pgid, ver});
		}

		if(ver < m_stateTip.verBase && m_stateTip.pgidStartPage != PGID_INVALID)
		{
			// all the scanning jobs done. quit back scan

			break;
		}
	}
	if(m_stateTip.pgidStartPage == PGID_INVALID)
	{
		PTNK_THROW_RUNTIME_ERR("TPIO::restoreState: could not find start page");	
	}

	// 2. sort the pages found while scan
	std::sort(pagevers.begin(), pagevers.end());
	VPageVer::const_reverse_iterator it = pagevers.rbegin(), itVerStart, itE = pagevers.rend();
	itVerStart = it;
	tx_id_t verCurrent = m_stateTip.verBase;
	Buffer bufStreak; bufStreak.reset();
	//    for each pages found in the scan, sorted by its version,
	//    add ovr entries and handle streak data per tx
	for(; it != itE; ++ it)
	{
		Page pg(m_backend->readPage(it->pgid));
		// std::cout << "ver: " << it->ver << " pgid: " << it->pgid << std::endl;
	
		// - ovr entries
		if(it->ver != m_stateTip.verBase)
		{
			page_id_t orig = pg.pageOvrTgt();
			if(orig != PGID_INVALID)
			{
				// new ovr entry found
				m_ovrs.add(orig, it->pgid, it->ver);
			}
		}

		// - page streaks
		if(it->ver != verCurrent)
		{
			handleStreak(bufStreak.rref());
			
			bufStreak.reset();
			verCurrent = it->ver;
		}
		if(pg.pageType() != PT_OVFLSTREAK)
		{
			BufferCRef pgstreak(pg.streak(), Page::STREAK_SIZE);
			// std::cout << "streak: " << pgstreak.hexdump() << std::endl;
			bufStreak.append(pgstreak);
		}
		else
		{
			OverflowedStreakPage ospg(pg);
			bufStreak.append(ospg.read());
		}
	}
	handleStreak(bufStreak.rref());
}

TPIO::~TPIO()
{
	/* NOP */
}

TPIOTxSession*
TPIO::newTransaction()
{
	return new TPIOTxSession(this, TPIOTxSession::SESSION_NORMAL);
}

TPIOTxSession::TPIOTxSession(TPIO* tpio, session_type_t type)
:	m_tpio(tpio), m_backend(tpio->m_backend.get()), m_txid(TXID_INVALID), m_type(type), m_numUniquePagesLocal(0)
{
	MUTEXPROF_START("TPIOTxSession C-tor");
	boost::shared_lock<boost::shared_mutex> g(tpio->m_mtxState);
	MUTEXPROF_END;

	m_state = tpio->m_stateTip;
}

TPIOTxSession::~TPIOTxSession()
{
	/* NOP */
}

pair<Page, page_id_t>
TPIOTxSession::newPage()
{
	++ m_numUniquePagesLocal;

	pair<Page, page_id_t> ret = m_backend->newPage();
	ret.first.setCacheStatus(ST_TX_LOCAL);

	return ret;
}

Page
TPIOTxSession::readPage(page_id_t pgid)
{
	++ m_stat.nRead;

#ifdef PTNK_BENCH_NOOVR
	page_id_t pgidOvr = pgid;
	Page pg = m_backend->readPage(pgid);
#else
	bool isOvr = false;
	bool wasCached = false; page_id_t pgidOvr = pgid;
	boost::tie(wasCached, pgidOvr) = m_cache.lookup(pgid);
	if(wasCached)
	{
		isOvr = (pgidOvr != pgid);
	}
	else
	{
		pgidOvr = m_ovrsTxLocal.find(pgid);
		if(pgidOvr != pgid)
		{
			// local override found
			++ m_stat.nReadOvrLocal;

			isOvr = true;
		}
		else
		{
			pgidOvr = m_tpio->m_ovrs.find(pgid, m_state.verBase, m_state.ver, m_state.ovrshint);
			if(pgidOvr != pgid)
			{
				// committed override found
				++ m_stat.nReadOvr;

				isOvr = true;
			}
		}

		m_cache.add(pgid, pgidOvr);
	}

	Page pg = m_backend->readPage(pgidOvr);
	// Page pg = ((PageIOMem*)(m_backend)->*&PageIOMem::readPage)(pgidOvr);
	pg.setIsBase(! isOvr);
#endif

	if(m_pagesModified.find(pgidOvr) != m_pagesModified.end())
	{
		// local mod
		pg.setCacheStatus(ST_TX_LOCAL);	
	}
	else
	{
		// not local mod
		pg.setCacheStatus(ST_PUBLISHED);
	}

	return pg;
}

Page
TPIOTxSession::modifyPage(const Page& page, mod_info_t* mod)
{
	++ m_stat.nModifyPage;

	if(page.cacheStatus() > ST_TX_LOCAL)
	{
		++ m_stat.nNewOvr;

		mod->idOrig = page.pageOrigId();

		Page ovr;
		boost::tie(ovr, mod->idOvr) = newPage();
		-- m_numUniquePagesLocal;

		ovr.makePageOvr(page, mod->idOvr);
		ovr.setCacheStatus(ST_TX_LOCAL);

		m_ovrsTxLocal.add(mod->idOrig, mod->idOvr);
		m_cache.add(mod->idOrig, mod->idOvr); // invalidate cache
		// mod->dump();
		
		return ovr;
	}
	else
	{
		mod->reset();

		return page;
	}
}

void
TPIOTxSession::discardPage(page_id_t pgid, mod_info_t* mod)
{
	if(mod)
	{
		mod->idOrig = pgid;
		mod->idOvr = PGID_INVALID;
	}

	m_ovrsTxLocal.add(pgid, PGID_INVALID);
}

void
TPIOTxSession::sync(page_id_t pgid)
{
	++ m_stat.nSync;
	
	m_pagesModified.insert(pgid);
	// sync to backend is delayed to after commit
	// m_backend->sync(pgid);
}

page_id_t
TPIOTxSession::getLastPgId() const
{
	return m_backend->getLastPgId();
}

void
TPIOTxSession::notifyPageWOldLink(page_id_t pgid, page_id_t pgidDep)
{
	++ m_stat.nNotifyOldLink;

	m_oldlinkLocal.add(pgid);
}

page_id_t
TPIOTxSession::updateLink(page_id_t idOld)
{
	PTNK_THROW_LOGIC_ERR("updateLink called in normal(non-rebase) tx");
}

void
TPIOTxSession::syncDelayed()
{
#ifdef TXSESSION_BATCH_SYNC
	if(! m_pagesModified.empty())
	{
		Spage_id_t::iterator it = m_pagesModified.begin(), itE = m_pagesModified.end();
		page_id_t pgidFirst = *it++;
		page_id_t pgidLast = pgidFirst;
		for(; it != itE; ++ it)
		{
			const page_id_t pgid = *it;

			if(PTNK_LIKELY(pgid == pgidLast + 1))
			{
				pgidLast = pgid;
			}
			else
			{
				m_backend->syncRange(pgidFirst, pgidLast);
				pgidLast = pgidFirst = pgid;
			}
		}
		m_backend->syncRange(pgidFirst, pgidLast);
	}
#else
	BOOST_FOREACH(page_id_t pgid, m_pagesModified)
	{
		m_backend->sync(pgid);	
	}
#endif
}

void
TPIOTxSession::fillTPIOHeaders()
{
	tx_id_t txid = m_txid;
	BOOST_FOREACH(page_id_t pgid, m_pagesModified)
	{
		mod_info_t mod;
		Page pgLast(m_backend->modifyPage(m_backend->readPage(pgid), &mod)); 

		pgLast.hdr()->txid = txid;
		pgLast.hdr()->flags = page_hdr_t::PF_VALID;
	}

	page_id_t pgidLast = *m_pagesModified.rbegin();

	mod_info_t mod;
	Page pgLast(m_backend->modifyPage(m_backend->readPage(pgidLast), &mod)); 

	page_hdr_t::flags_t flags = page_hdr_t::PF_VALID | page_hdr_t::PF_END_TX;
	if(m_type == SESSION_REBASE) flags |= page_hdr_t::PF_TX_REBASE;

	pgLast.hdr()->flags = flags;
}

bool
TPIOTxSession::tryCommit()
{
	if(m_pagesModified.empty())
	{
		return true;	
	}

	// check conflicts and commit
	tx_id_t verTx;
	uint64_t numUniquePages;
	{
		boost::unique_lock<boost::mutex> g(m_tpio->m_mtx, boost::defer_lock_t());
		if(m_type != SESSION_REBASE)
		{
			// rebase tx have already m_mtx locked
			MUTEXPROF_START("tryCommit m_tpio->m_mtx");
			g.lock();
			MUTEXPROF_END;
		}

		if(m_type != SESSION_REFRESH)
		{
			if(! m_tpio->m_ovrs.checkConflict(m_ovrsTxLocal, m_state.verBase, m_state.ver, m_state.ovrshint))
			{
				// tx fail
				++ m_stat.nCommitFail;

				return false;
			}
		}
		else // if m_type == SESSION_REFRESH
		{
			if(! m_tpio->m_ovrs.filterConflict(m_ovrsTxLocal, m_state.verBase, m_state.ver, m_state.ovrshint))
			{
				// tx fail
				++ m_stat.nCommitFail;

				return false;
			}
		}

		if(m_type == SESSION_REBASE)
		{
			MUTEXPROF_START("tryCommit SESSION_REBASE m_tpio->m_mtxState (excl)");
			boost::unique_lock<boost::shared_mutex> gt(m_tpio->m_mtxState);
			MUTEXPROF_END;

			verTx = m_tpio->m_stateTip.ver + 1;
			PTNK_ASSERT(verTx > m_state.ver);
			PTNK_ASSERT(verTx > m_state.verBase);

			m_tpio->m_stateTip.ver = verTx;
			m_tpio->m_stateTip.verBase = verTx;
			m_tpio->m_stateTip.ovrshint = m_tpio->m_ovrs.getHint();
			m_tpio->m_stateTip.pgidStartPage = pgidStartPage();
			m_tpio->m_numUniquePages += m_numUniquePagesLocal;
			numUniquePages = m_tpio->m_numUniquePages;

			gt.unlock();

			m_tpio->m_oldlink.clear();
		}
		else
		{
			MUTEXPROF_START("tryCommit non-SESSION_REBASE m_tpio->m_mtxState");
			boost::upgrade_lock<boost::shared_mutex> gt(m_tpio->m_mtxState);
			MUTEXPROF_END;

			if(m_tpio->m_stateTip.verBase != m_state.verBase)
			{
				// tx should not cross rebase
				return false;
			}
			verTx = m_tpio->m_stateTip.ver + 1;
			PTNK_ASSERT(verTx > m_state.ver);
			PTNK_ASSERT(verTx > m_state.verBase);

			// merge ovr -> TPIO
			m_tpio->m_ovrs.merge(m_ovrsTxLocal, m_state.verBase, m_state.ver, verTx);

			{
				MUTEXPROF_START("tryCommit non-SESSION_REBASE m_tpio->m_mtxState (excl)");
				boost::upgrade_to_unique_lock<boost::shared_mutex> gtu(gt);
				MUTEXPROF_END;

				m_tpio->m_stateTip.ver = verTx;
				m_tpio->m_stateTip.pgidStartPage = pgidStartPage();
				m_tpio->m_numUniquePages += m_numUniquePagesLocal;
				numUniquePages = m_tpio->m_numUniquePages;
			}
			gt.unlock();

			m_tpio->m_oldlink.merge(m_oldlinkLocal);
		}
	}
	m_txid = verTx;

	// write streaks
	{
		StreakIO<Spage_id_t::const_iterator> sio(m_pagesModified.begin(), m_pagesModified.end(), this);
		sio.write(BufferCRef(&numUniquePages, sizeof(uint64_t)));

		if(m_type == SESSION_REBASE)
		{
			m_oldlinkLocal.clear();
		}
		m_oldlinkLocal.dump(sio);
	}

	// fill tpio related headers on the last page to be written
	fillTPIOHeaders();

	// actually write pages to disk
	syncDelayed();

	return true;
}

void
TPIOTxSession::Stat::dump() const
{
	std::cout << "** TxSession::Stat dump **" << std::endl;
	std::cout << "nRead:\t\t" << nRead << std::endl;
	std::cout << "nReadOvr:\t" << nReadOvr << std::endl;
	std::cout << "nReadOvrLocal:\t" << nReadOvrLocal << std::endl;
	std::cout << "nModifyPage:\t" << nModifyPage << std::endl;
	std::cout << "nNewOvr:\t" << nNewOvr << std::endl;
	std::cout << "nSync:\t\t" << nSync << std::endl;
	std::cout << "nNotifyOldLink:\t" << nNotifyOldLink << std::endl;
	std::cout << "nCommitFail:\t" << nCommitFail << std::endl;

	std::cout << std::endl;
}

// #define VERBOSE_REBASE

page_id_t
TPIO::RebaseTPIOTxSession::updateLink(page_id_t idOld)
{
	page_id_t idR = rebaseVisit(idOld);
	if(idR != idOld) return idR;

	page_id_t idL = m_ovrsTxLocal.find(idOld);
	if(idL != idOld) return idL;

	page_id_t idO = m_tpio->m_ovrs.find(idOld, m_state.verBase, m_state.ver, m_state.ovrshint);
	if(idO != idOld) return idO;
	
	return idOld;
}

inline
page_id_t
TPIO::RebaseTPIOTxSession::rebaseForceVisit(page_id_t pgid)
{
	m_visited.insert(pgid);

#ifdef VERBOSE_REBASE
	std::cout << "update link for pgid: " << pgid << std::endl;
#endif
	
	Page pg(readPage(pgid));

	mod_info_t mod;
	pg.updateLinks(&mod, this);
	
	return mod.isValid() ? mod.idOvr : pgid;
}

page_id_t
TPIO::RebaseTPIOTxSession::rebaseVisit(page_id_t pgid)
{
	if(! m_tpio->m_oldlink.contains(pgid))
	{
		// no need to visit
		return pgid;
	}

	if(m_visited.find(pgid) != m_visited.end())
	{
		// already visited
		return pgid;	
	}

	return rebaseForceVisit(pgid);
}

void
TPIO::rebase(bool force)
{
	if(! force)
	{
		ssize_t left = m_ovrs.getSpaceLeft(m_stateTip.verBase, m_stateTip.ovrshint);
		if(left > REBASE_THRESHOLD)
		{
			// no need to rebase yet
			return;
		}
	}

#ifdef VERBOSE_REBASE
	printf("rebase start verBase: %d verLatest: %d \n", m_stateTip.verBase, m_stateTip.ver);
#endif
	for(;;)
	{
#ifdef VERBOSE_REBASE
		printf("rebase tx start\n");
#endif
		boost::lock_guard<boost::mutex> g(m_mtx);
		boost::scoped_ptr<RebaseTPIOTxSession> tx(new RebaseTPIOTxSession(this));
		
		tx->setPgidStartPage(tx->rebaseForceVisit(tx->pgidStartPage()));

#ifdef VERBOSE_REBASE
		tx->dumpStat();
#endif
		if(tx->tryCommit()) break;

#ifdef VERBOSE_REBASE
		std::cout << "rebase tx failed to commit" << std::endl;
		dumpStat();
#endif
	}
#ifdef VERBOSE_REBASE
	printf("rebase end verBase: %d verLatest: %d \n", m_stateTip.verBase, m_stateTip.ver);
#endif
}

#define VERBOSE_REFRESH

void
TPIO::refreshOldPages(page_id_t threshold)
{
#ifdef VERBOSE_REFRESH
	printf("refresh start verBase: %d verLatest: %d \n", m_stateTip.verBase, m_stateTip.ver);
#endif
	for(;;)
	{
#ifdef VERBOSE_REFRESH
		printf("refresh tx start\n");
#endif
		boost::scoped_ptr<RefreshTPIOTxSession> tx(new RefreshTPIOTxSession(this));

		Page pgStart(tx->readPage(tx->pgidStartPage()));
		
		void* cursor = NULL;
		static const int MAX_PAGES = INT_MAX; // FIXME!
		pgStart.refreshAllLeafPages(&cursor, threshold, MAX_PAGES, PGID_INVALID, tx.get());

#ifdef VERBOSE_REFRESH
		tx->dumpStat();
#endif
		if(tx->tryCommit()) break;

#ifdef VERBOSE_REFRESH
		std::cout << "refresh tx failed to commit" << std::endl;
		dumpStat();
#endif
	}
#ifdef VERBOSE_REFRESH
	printf("refresh end verBase: %d verLatest: %d \n", m_stateTip.verBase, m_stateTip.ver);
#endif
}

void
TPIO::dumpStat() const
{
	m_backend->dumpStat();
	std::cout << "** TPIO stat dump **" << std::endl;
	m_ovrs.dumpStat();
	std::cout << "verLatest: " << m_stateTip.ver << std::endl;
	std::cout << "verBase: " << m_stateTip.verBase << std::endl;
	std::cout << "number of pgs w/ old links: " << m_oldlink.m_impl.size() << std::endl;
	std::cout << "numUniquePages: " << m_numUniquePages << std::endl;
}

} // end of namespace ptnk
