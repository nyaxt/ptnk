#include "tpio2.h"
#include "streak.h"
#include "sysutils.h"

#define TXSESSION_BATCH_SYNC
// #define DEBUG_VERBOSE_TPIOPIO
// #define DEBUG_VERBOSE_RESTORESTATE
// #define VERBOSE_REBASE
#define VERBOSE_REFRESH

namespace ptnk
{

#define ADDEXACT(V) { unsigned int tmp; do { tmp = V; } while(! __sync_bool_compare_and_swap(&V, tmp, tmp+o.V)); }
#define ADD(V) { V += o.V; }

void
TPIOStat::merge(const TPIOStat& o)
{
	ADDEXACT(nUniquePages)
	ADD(nRead)
	ADD(nReadOvr)
	ADD(nReadOvrLocal)
	ADD(nModifyPage)
	ADD(nOvr)
	ADD(nSync)
	ADD(nNotifyOldLink)
}

void
TPIOStat::dump(std::ostream& s) const
{
	s << "  nUniquePages:\t" << nUniquePages << std::endl;
	s << "  nRead:\t" << nRead << std::endl;
	s << "  nReadOvr:\t" << nReadOvr << std::endl;
	s << "  nReadOvrL:\t" << nReadOvrLocal << std::endl;
	s << "  nModifyPage:\t" << nModifyPage << std::endl;
	s << "  nOvr:\t" << nOvr << std::endl;
	s << "  nSync:\t" << nSync << std::endl;
	s << "  nNotifyOldLink:\t" << nNotifyOldLink << std::endl;
}

TPIO2TxSession::TPIO2TxSession(TPIO2* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr)
:	m_tpio(tpio),
	m_aovr(move(aovr)),
	m_lovr(move(lovr))
{
	m_lovr->attachExtra(unique_ptr<OvrExtra>(new OvrExtra));
	m_oldlink = &reinterpret_cast<OvrExtra*>(m_lovr->getExtra())->oldlink;
}

void
TPIO2TxSession::dump(std::ostream& s) const
{
	s << "** TPIO2TxSession dump **" << std::endl;
	s << m_stat;
}

TPIO2TxSession::OvrExtra::~OvrExtra()
{
	/* NOP */
}

TPIO2TxSession::~TPIO2TxSession()
{
	/* NOP */
}

pair<Page, page_id_t>
TPIO2TxSession::newPage()
{
	++ m_stat.nUniquePages;

	pair<Page, page_id_t> ret = backend()->newPage();

	return ret;
}

Page
TPIO2TxSession::readPage(page_id_t pgid)
{
	++ m_stat.nRead;

	MUTEXPROF_START("resolveOvr");
	page_id_t pgidOvr; ovr_status_t st;
	tie(pgidOvr, st) = m_lovr->searchOvr(pgid);
#ifdef DEBUG_VERBOSE_TPIOPIO
	std::cout << "readPage " << pgid2str(pgid) << " -> " << pgid2str(pgidOvr) << " st: " << st << std::endl;
#endif
	MUTEXPROF_END;

	Page pg = backend()->readPage(pgidOvr);
	if(st != OVR_NONE)
	{
		// pg is override page
		pg.setIsBase(false);

		++ m_stat.nReadOvr;

		if(st == OVR_LOCAL)
		{
			// if tx local ovr. pg is mutable
			pg.setMutable();

			++ m_stat.nReadOvrLocal;
		}
	}
	else
	{
		// pg is not override page
		pg.setIsBase(true);
	}

	return pg;
}

Page
TPIO2TxSession::modifyPage(const Page& page, mod_info_t* mod)
{
	++ m_stat.nModifyPage;

	if(! page.isMutable())
	{
		++ m_stat.nOvr;

		mod->idOrig = page.pageOrigId();

		Page ovr;
		tie(ovr, mod->idOvr) = newPage();
		-- m_stat.nUniquePages;

		MUTEXPROF_START("makePageOvr");
		ovr.makePageOvr(page, mod->idOvr);
		MUTEXPROF_END;

		addOvr(mod->idOrig, mod->idOvr);
		
#ifdef DEBUG_VERBOSE_TPIOPIO
		std::cout << "modifyPage " << pgid2str(mod->idOrig) << " -> " << pgid2str(mod->idOvr) << std::endl;
#endif
		return ovr;
	}
	else
	{
#ifdef DEBUG_VERBOSE_TPIOPIO
		std::cout << "modifyPage " << pgid2str(page.pageOrigId()) << " (already mutable)" << std::endl;
#endif
		mod->reset();

		return page;
	}
}

void
TPIO2TxSession::discardPage(page_id_t pgid, mod_info_t* mod)
{
	if(mod)
	{
		mod->idOrig = pgid;
		mod->idOvr = PGID_INVALID;
	}

	m_lovr->addOvr(pgid, PGID_INVALID); // FIXME: is this really needed?
}

void
TPIO2TxSession::sync(page_id_t pgid)
{
	++ m_stat.nSync;
	
#ifdef DEBUG_VERBOSE_TPIOPIO
	std::cout << "sync " << pgid2str(pgid) << std::endl;
#endif
	m_pagesModified.push_back(pgid);
	// sync to backend is delayed to after commit
}

page_id_t
TPIO2TxSession::getLastPgId() const
{
	return backend()->getLastPgId();
}

void
TPIO2TxSession::notifyPageWOldLink(page_id_t pgid)
{
	++ m_stat.nNotifyOldLink;

	oldlink()->add(pgid);
}

page_id_t
TPIO2TxSession::updateLink(page_id_t pgidOld)
{
	PTNK_THROW_LOGIC_ERR("updateLink called in normal(non-rebase) tx");
}

void
TPIO2TxSession::loadStreak(BufferCRef bufStreak)
{
	if(bufStreak.empty()) return;

	bufStreak.popFrontTo(&m_stat.nUniquePages, sizeof(uint64_t));
	oldlink()->restore(bufStreak);
}

TPIO2::TPIO2(shared_ptr<PageIO> backend, ptnk_opts_t opts)
:	m_backend(backend),
	m_sync(opts & OAUTOSYNC),
	m_bDuringRebase(false)
{
	if(m_backend->needInit())
	{
		m_aovr = unique_ptr<ActiveOvr>(new ActiveOvr);
	}
	else
	{
		// need to restore state
		restoreState();
	}
}

TPIO2::~TPIO2()
{
	/* NOP */
}

unique_ptr<TPIO2TxSession>
TPIO2::newTransaction()
{
	// wait while rebase is processed
	while(m_bDuringRebase)
	{
		MUTEXPROF_START("waitrebase");
		boost::unique_lock<boost::mutex> g(m_mtxRebase);

		PTNK_MEMBARRIER_COMPILER;
		if(m_bDuringRebase)
		{
			m_condRebase.wait(g);
		}
		MUTEXPROF_END;
	}

	shared_ptr<ActiveOvr> aovr;
	{
		boost::shared_lock<boost::shared_mutex> g(m_mtxAOvr);
		aovr = m_aovr;
	}
	unique_ptr<LocalOvr> lovr = aovr->newTx();
	return unique_ptr<TPIO2TxSession>(new TPIO2TxSession(this, move(aovr), move(lovr)));
}

void
TPIO2::syncDelayed(const Vpage_id_t& pagesModified)
{
	if(! m_sync) return;

#ifdef TXSESSION_BATCH_SYNC
	if(! pagesModified.empty())
	{
		auto it = pagesModified.begin(), itE = pagesModified.end();
		page_id_t pgidFirst = *it++;
		page_id_t pgidLast = pgidFirst;
		for(; it != itE; ++ it)
		{
			const page_id_t pgid = *it;
			if(pgid == pgidLast) continue; // skip duplicates

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
	for(page_id_t pgid: pagesModified)
	{
		m_backend->sync(pgid);	
	}
#endif
}

void
TPIO2::commitTxPages(TPIO2TxSession* tx, ver_t verW, bool isRebase)
{
	// sort modified pages ary
	Vpage_id_t& pagesModified = tx->m_pagesModified;
	std::sort(pagesModified.begin(), pagesModified.end());

	// write streaks
	{
		StreakIO<Vpage_id_t::const_iterator> sio(pagesModified.begin(), pagesModified.end(), backend());
		sio.write(BufferCRef(&m_stat.nUniquePages, sizeof(uint64_t)));
		tx->oldlink()->dump(sio);
	}

	// fill tpio header
	// NOTE: this assumes mmap-ed pageio impl
	{
		for(page_id_t pgid: pagesModified)
		{
			Page pgLast(m_backend->readPage(pgid));

			pgLast.hdr()->txid = verW;
			pgLast.hdr()->flags = page_hdr_t::PF_VALID;
		}

		// last page of tx w/ special flag
		{
			page_id_t pgidLast = pagesModified.back();
			Page pgLast(m_backend->readPage(pgidLast));

			page_hdr_t::flags_t flags = page_hdr_t::PF_VALID | page_hdr_t::PF_END_TX;
			if(isRebase) flags |= page_hdr_t::PF_TX_REBASE;

			pgLast.hdr()->flags = flags;
		}
	}

	// write pages to disk
	syncDelayed(pagesModified);
}

bool
TPIO2::tryCommit(TPIO2TxSession* tx, commit_flags_t flags)
{
	if(tx->m_pagesModified.empty())
	{
		// no write in tx
		return true;
	}

	// 1. try committing ovr info
	ver_t verW;
	{
		MUTEXPROF_START("aovr tryCommit");
		if((verW = tx->m_aovr->tryCommit(tx->m_lovr, flags)) == TXID_INVALID)
		{
			return false;
		}
		MUTEXPROF_END;
	}

	// ovr info committed! now tx is guaranteed to be valid...
	
	// 2. update stat data
	m_stat.merge(tx->m_stat);

	// 3. fill pages info / write pages to disk
	commitTxPages(tx, verW, false);

	return true;
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

#define PTNK_BKWD_SCAN(pio) \
	page_id_t pgid = pio->getLastPgId(); \
	for(part_id_t partid = PGID_PARTID(pgid); partid != PTNK_PARTID_INVALID; -- partid) \
	for(pgid = PGID_PARTLOCAL(partid, pio->getPartLastLocalPgId(partid)); PGID_LOCALID(pgid) != PGID_LOCALID_MASK; -- pgid)

void
TPIO2::restoreState()
{
	page_id_t pgidStartPage = PGID_INVALID;
	// find last rebase tx and restore ovrs table	

	// 1. scan the log backwards until rebase tx AND start page is found
	// remember the pages found in the process -> pagevers
#ifdef DEBUG_VERBOSE_RESTORESTATE
	printf("restore phase1 start\n");
#endif
	ver_t verBase = 1;
	VPageVer pagevers;
	PTNK_BKWD_SCAN(m_backend)
	{
		Page pg(m_backend->readPage(pgid));

		page_hdr_t::flags_t flags = pg.hdr()->flags;
		tx_id_t ver = pg.hdr()->txid;

		// skip invalid page
		if(! pg.isCommitted()) continue;

#ifdef DEBUG_VERBOSE_RESTORESTATE
		std::cout << "ver: " << ver << " scan valid pg: " << pgid2str(pgid) << std::endl;
#endif

		if(pgidStartPage == PGID_INVALID && pg.pageType() == PT_DB_OVERVIEW)
		{
			// start page found.
			pgidStartPage = pgid;

#ifdef DEBUG_VERBOSE_RESTORESTATE
		    std::cout << "- " << pgid2str(pgidStartPage) << " as startpg" << std::endl;
#endif
		}

		if((flags & page_hdr_t::PF_END_TX) && (flags & page_hdr_t::PF_TX_REBASE))
		{
			// rebase tx found.
			verBase = ver;

			// FIXME: This code assumes that no concurrent tx cross rebase tx pages
			//        However, this may not be the case in future.

#ifdef DEBUG_VERBOSE_RESTORESTATE
		    std::cout << "- as rebase tx" << std::endl;
#endif
		}
		
		pagevers.push_back((PageVer){pgid, ver});

		if(ver < verBase && pgidStartPage != PGID_INVALID)
		{
			// all the scanning jobs done. quit back scan
			goto SCANDONE;
		}
	}
	SCANDONE:;
	if(pgidStartPage == PGID_INVALID)
	{
		PTNK_THROW_RUNTIME_ERR("TPIO::restoreState: could not find start page");	
	}
#ifdef DEBUG_VERBOSE_RESTORESTATE
	std::cout << "restore phase1 end. pgidStartPage: " << pgid2str(pgidStartPage) << " verBase: " << verBase << std::endl;
#endif

	// 2. sort the pages found while scan by its version
	std::sort(pagevers.begin(), pagevers.end());

	// 3. for each pages found in the scan, sorted by its version,
	//    add ovr entries and handle streak data per tx
	{
		boost::unique_lock<boost::shared_mutex> g(m_mtxAOvr);
		m_aovr = shared_ptr<ActiveOvr>(new ActiveOvr(pgidStartPage, verBase));
	}
	unique_ptr<TPIO2TxSession> tx = newTransaction();
	tx_id_t verCurrent = verBase;
	Buffer bufStreak; bufStreak.reset();

	VPageVer::const_reverse_iterator it = pagevers.rbegin(), itE = pagevers.rend();
	for(; it != itE; ++ it)
	{
		if(it->ver < verBase) continue;

		if(it->ver != verCurrent)
		{
			// new tx begins...
#ifdef DEBUG_VERBOSE_RESTORESTATE
			printf("replay tx %d before new one begins\n", verCurrent);
#endif
			// commit current tx
			// -- streak
			tx->loadStreak(bufStreak.rref());
			bufStreak.reset();

			// -- ovr entries
			if(verCurrent != verBase)
			{
				PTNK_CHECK(verCurrent == m_aovr->tryCommit(tx->m_lovr, COMMIT_REPLAY, verCurrent));
			}
			tx = newTransaction();

			verCurrent = it->ver;
		}

		Page pg(m_backend->readPage(it->pgid));
#ifdef DEBUG_VERBOSE_RESTORESTATE
		std::cout << "ver: " << it->ver << " pgid: " << pgid2str(it->pgid) << std::endl;
#endif
	
		// -- ovr entries
		if(it->ver > verBase)
		{
			page_id_t orig = pg.pageOvrTgt();
			if(orig != PGID_INVALID)
			{
				// new ovr entry found
#ifdef DEBUG_VERBOSE_RESTORESTATE
				printf("- add ovr entry %d -> %d\n", orig, it->pgid);
#endif
				tx->addOvr(orig, it->pgid);
			}
		}

		// -- page streaks
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
#ifdef DEBUG_VERBOSE_RESTORESTATE
	printf("replay tx %d (last one)\n", verCurrent);
#endif
	tx->loadStreak(bufStreak.rref());
	m_stat.nUniquePages = tx->m_stat.nUniquePages; // FIXME
	if(verCurrent != verBase)
	{
		PTNK_CHECK(verCurrent == m_aovr->tryCommit(tx->m_lovr, COMMIT_REPLAY, verCurrent));
	}
}

TPIO2::RebaseTPIO2TxSession::RebaseTPIO2TxSession(TPIO2* tpio, shared_ptr<ActiveOvr> aovr, unique_ptr<LocalOvr> lovr, PagesOldLink* oldlink)
:	TPIO2TxSession(tpio, move(aovr), move(lovr)),
	m_oldlinkRebase(oldlink)
{
	/* NOP */
}

TPIO2::RebaseTPIO2TxSession::~RebaseTPIO2TxSession()
{
	/* NOP */
}

page_id_t
TPIO2::RebaseTPIO2TxSession::rebaseForceVisit(page_id_t pgid)
{
	m_visited.insert(pgid);
	
#ifdef VERBOSE_REBASE
	std::cout << "rebase visit pgid: " << pgid << std::endl;
#endif

	Page pg(readPage(pgid));

	mod_info_t mod;
	pg.updateLinks(&mod, this);
	
	return mod.isValid() ? mod.idOvr : pgid;
}

page_id_t
TPIO2::RebaseTPIO2TxSession::rebaseVisit(page_id_t pgid)
{
	if(! m_oldlinkRebase->contains(pgid))
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

page_id_t
TPIO2::RebaseTPIO2TxSession::updateLink(page_id_t idOld)
{
	page_id_t idR = rebaseVisit(idOld);
	if(idR != idOld)
	{
#ifdef VERBOSE_REBASE
		std::cout << "updateLink: " << pgid2str(idOld) << " -> (rebase visited) " << pgid2str(idR) << std::endl;	
#endif
		return idR;
	}

	page_id_t idO = m_lovr->searchOvr(idOld).first;
#ifdef VERBOSE_REBASE
	std::cout << "updateLink: " << pgid2str(idOld) << " -> " << pgid2str(idO) << std::endl;	
#endif
	return idO;
}

void
TPIO2::rebase(bool force)
{
	if(m_bDuringRebase) return; // already during rebase

	if(!force && m_stat.nOvr < REBASE_THRESHOLD) return; // num ovrs below threshold

	if(! __sync_bool_compare_and_swap(&m_bDuringRebase, false, true)) return;

#ifdef VERBOSE_REBASE
	printf("rebase start\n");
	std::cout << *this;
#endif

	// 1. refuse further tx to commit
	m_aovr->terminate();
	
	// 2. ready list of pages old link
	PagesOldLink pol;
	{
		MUTEXPROF_START("rebase:pol");
		for(LocalOvr* o = m_aovr->lovrVerifiedTip(); o; o = o->prev())
		{
			if(! o->isMerged()) continue; // skip terminator

			const PagesOldLink& opol = reinterpret_cast<TPIO2TxSession::OvrExtra*>(o->getExtra())->oldlink;
			pol.merge(opol);
		}
		MUTEXPROF_END;
	}
#ifdef VERBOSE_REBASE
	std::cerr << pol << std::endl;
#endif

	// 3. create rebsae tx. and start visit from root
	ver_t verBase;
	unique_ptr<RebaseTPIO2TxSession> tx;
	{
		shared_ptr<ActiveOvr> aovr = m_aovr;
		unique_ptr<LocalOvr> lovr(aovr->newTx());
		verBase = lovr->verRead() + 1;
		// ver_t verB2 = m_aovr->lovrVerifiedTip()->prev()->verWrite() + 1;
		// PTNK_CHECK(verBase == verB2);
		tx.reset(new RebaseTPIO2TxSession(this, move(aovr), move(lovr), &pol));
	}
	
	{
		MUTEXPROF_START("rebase:visit");
		tx->setPgidStartPage(tx->rebaseForceVisit(tx->pgidStartPage()));
		MUTEXPROF_END;
	}

#ifdef VERBOSE_REBASE
	std::cerr << *tx << std::endl;
#endif

	// 4. commit rebase tx. pages
	commitTxPages(tx.get(), verBase, true);
	
	// 5. trash old aovr and start accepting new tx
	m_stat.nOvr = 0; // clear num ovr.
	// FIXME FIXME: shared_ptr can't be assigned atomically (refcnt / ptr)
	m_aovr = shared_ptr<ActiveOvr>(new ActiveOvr(tx->pgidStartPage(), verBase));

	PTNK_MEMBARRIER_COMPILER;

	m_bDuringRebase = false;
	m_condRebase.notify_all();
	
#ifdef VERBOSE_REBASE
	printf("rebase end verBase: %d\n", m_aovr->verBase());
	std::cout << *this;
#endif
}

void
TPIO2::refreshOldPages(page_id_t threshold)
{
	if(m_bDuringRefresh) return; // already during refresh
	if(! __sync_bool_compare_and_swap(&m_bDuringRefresh, false, true)) return;

#ifdef VERBOSE_REFRESH
	std::cout << "refresh start" << std::endl << *this;
#endif
	
	void* cursor = NULL;
	{
		unique_ptr<TPIO2TxSession> tx(newTransaction());

		Page pgStart(tx->readPage(tx->pgidStartPage()));
		
		static const int MAX_PAGES = INT_MAX; // FIXME!
		pgStart.refreshAllLeafPages(&cursor, threshold, MAX_PAGES, PGID_INVALID, tx.get());

#ifdef VERBOSE_REFRESH
		tx->dumpStat();
#endif
		if(! tryCommit(tx.get())) //, COMMIT_REFRESH))
		{
			std::cerr << "refresh ci failed!" << std::endl;	
		}
	}

#ifdef VERBOSE_REFRESH
	std::cout << "refresh end" << std::endl << *this;
#endif

	rebase(/* force = */ true);
}

void
TPIO2::dump(std::ostream& s) const
{
	s << "** TPIO dump **" << std::endl;
	s << m_stat;
	m_backend->dumpStat(); // FIXME!
}

} // end of namespace ptnk
