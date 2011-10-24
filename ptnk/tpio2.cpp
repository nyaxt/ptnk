#include "tpio2.h"

#include <boost/tuple/tuple.hpp> // boost::tie

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

		m_lovr->addOvr(mod->idOrig, mod->idOvr);
		
		return ovr;
	}
	else
	{
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

	getOldLink()->add(pgid);
}

page_id_t
TPIO2TxSession::updateLink(page_id_t pgidOld)
{
	PTNK_THROW_LOGIC_ERR("updateLink called in normal(non-rebase) tx");
}

TPIO2::TPIO2(boost::shared_ptr<PageIO> backend)
:	m_backend(backend)
{
	m_aovr = new ActiveOvr;
}

TPIO2::~TPIO2()
{
	/* NOP */
}

TPIO2TxSession*
TPIO2::newTransaction()
{
	return new TPIO2TxSession(this, m_aovr->newTx());
}

bool
TPIO2::tryCommit(TPIO2TxSession* tx)
{
	if(tx->m_pagesModified.empty())
	{
		// no write in tx
		return true;
	}

	// try committing ovr info
	if(! m_aovr->tryCommit(tx->m_lovr))
	{
		return false;	
	}

	// ovr info committed!...
	// now do the page writes
	
	std::sort(tx->m_pagesModified.begin(), tx->m_pagesModified.end());
	// TODO:
	// fill tpio header
	// write streaks
	// sync!
	// etc. etc.
	
	return true;
}

void
TPIO2::rebase(bool force)
{
	std::cerr << "FIXME: TPIO2::rebase not yet implemented!" << std::endl;
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
