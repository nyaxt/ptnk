#include "tpio2.h"
#include "streak.h"

#define TXSESSION_BATCH_SYNC
// #define DEBUG_VERBOSE_TPIOPIO
#define DEBUG_VERBOSE_RESTORESTATE

namespace ptnk
{

TPIO2TxSession::TPIO2TxSession(TPIO2* tpio, unique_ptr<LocalOvr>&& lovr)
:	m_tpio(tpio),
	m_lovr(move(lovr))
{
	m_lovr->attachExtra(unique_ptr<OvrExtra>(new OvrExtra));
}

void
TPIO2TxSession::dump(std::ostream& s) const
{
	s << "** TPIO2TxSession dump **" << std::endl;
	s << "  nUniquePages:\t" << m_stat.nUniquePages << std::endl;
	s << "  nRead:\t" << m_stat.nRead << std::endl;
	s << "  nReadOvr:\t" << m_stat.nReadOvr << std::endl;
	s << "  nReadOvrL:\t" << m_stat.nReadOvrLocal << std::endl;
	s << "  nModifyPage:\t" << m_stat.nModifyPage << std::endl;
	s << "  nNewOvr:\t" << m_stat.nNewOvr << std::endl;
	s << "  nSync:\t" << m_stat.nSync << std::endl;
	s << "  nNotifyOldLink:\t" << m_stat.nNotifyOldLink << std::endl;
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

	page_id_t pgidOvr; ovr_status_t st;
	tie(pgidOvr, st) = m_lovr->searchOvr(pgid);
#ifdef DEBUG_VERBOSE_TPIOPIO
	// std::cout << "readPage " << pgid2str(pgid) << " -> " << pgid2str(pgidOvr) << " st: " << st << std::endl;
#endif

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
		++ m_stat.nNewOvr;

		mod->idOrig = page.pageOrigId();

		Page ovr;
		tie(ovr, mod->idOvr) = newPage();
		-- m_stat.nUniquePages;

		ovr.makePageOvr(page, mod->idOvr);

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

TPIO2::TPIO2(boost::shared_ptr<PageIO> backend)
:	m_backend(backend)
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
	return unique_ptr<TPIO2TxSession>(new TPIO2TxSession(this, m_aovr->newTx()));
}

void
TPIO2::syncDelayed(const Vpage_id_t& pagesModified)
{
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

bool
TPIO2::tryCommit(TPIO2TxSession* tx, ver_t verW)
{
	if(tx->m_pagesModified.empty())
	{
		// no write in tx
		return true;
	}

	// try committing ovr info
	PagesOldLink* oldlink = tx->oldlink(); // capture this here as tryCommit below would blow away tx->m_lovr used in retrieval
	if((verW = m_aovr->tryCommit(tx->m_lovr, verW)) == TXID_INVALID)
	{
		return false;
	}

	// ovr info committed!...
	// now do the page writes
	
	Vpage_id_t& pagesModified = tx->m_pagesModified;
	std::sort(pagesModified.begin(), pagesModified.end());

	// write streaks
	{
		StreakIO<Vpage_id_t::const_iterator> sio(pagesModified.begin(), pagesModified.end(), backend());
		sio.write(BufferCRef(&tx->m_stat.nUniquePages, sizeof(uint64_t)));
		oldlink->dump(sio);
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

		page_id_t pgidLast = pagesModified.back();

		Page pgLast(m_backend->readPage(pgidLast));

		page_hdr_t::flags_t flags = page_hdr_t::PF_VALID | page_hdr_t::PF_END_TX;
		//FIXME if(m_type == SESSION_REBASE) flags |= page_hdr_t::PF_TX_REBASE;

		pgLast.hdr()->flags = flags;
	}

	syncDelayed(pagesModified);

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

		if(pgidStartPage == PGID_INVALID && pg.pageType() == PT_DB_OVERVIEW)
		{
			// start page found.
			pgidStartPage = pgid;
		}

		if((flags & page_hdr_t::PF_END_TX) && (flags & page_hdr_t::PF_TX_REBASE))
		{
			// rebase tx found.
			verBase = ver;

			// FIXME: This code assumes that no concurrent tx cross rebase tx pages
			//        However, this may not be the case in future.
		}
		
		pagevers.push_back((PageVer){pgid, ver});

		if(ver < verBase && pgidStartPage != PGID_INVALID)
		{
			// all the scanning jobs done. quit back scan

			break;
		}
	}
	if(pgidStartPage == PGID_INVALID)
	{
		PTNK_THROW_RUNTIME_ERR("TPIO::restoreState: could not find start page");	
	}
#ifdef DEBUG_VERBOSE_RESTORESTATE
	printf("restore phase1 end. pgidStartPage: %d, verBase: %d\n", pgidStartPage, verBase);
#endif

	// 2. sort the pages found while scan by its version
	std::sort(pagevers.begin(), pagevers.end());

	// 3. for each pages found in the scan, sorted by its version,
	//    add ovr entries and handle streak data per tx
	m_aovr = unique_ptr<ActiveOvr>(new ActiveOvr(pgidStartPage, verBase));
	unique_ptr<TPIO2TxSession> tx = newTransaction();
	tx_id_t verCurrent = pagevers.back().ver;
	Buffer bufStreak; bufStreak.reset();

	VPageVer::const_reverse_iterator it = pagevers.rbegin(), itE = pagevers.rend();
	for(; it != itE; ++ it)
	{
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
			PTNK_CHECK(verCurrent == m_aovr->tryCommit(tx->m_lovr, verCurrent));
			tx = newTransaction();

			verCurrent = it->ver;
		}

		Page pg(m_backend->readPage(it->pgid));
#ifdef DEBUG_VERBOSE_RESTORESTATE
		std::cout << "ver: " << it->ver << " pgid: " << it->pgid << std::endl;
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
	PTNK_CHECK(verCurrent == m_aovr->tryCommit(tx->m_lovr, verCurrent));
}

void
TPIO2::rebase(bool force)
{
	// std::cerr << "FIXME: TPIO2::rebase not yet implemented!" << std::endl;
}

void
TPIO2::refreshOldPages(page_id_t threshold)
{
	std::cerr << "FIXME: TPIO2::refleshOldPages not yet implemented!" << std::endl;
}

void
TPIO2::dump(std::ostream& s) const
{
	s << "** TPIO dump **" << std::endl;
}

} // end of namespace ptnk
