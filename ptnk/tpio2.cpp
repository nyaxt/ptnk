#include "tpio2.h"

#include <boost/tuple/tuple.hpp> // boost::tie

namespace ptnk
{

TPIO2TxSession::TPIO2TxSession(TPIO2* tpio, unique_ptr<LocalOvr>&& lovr)
:	m_tpio(tpio),
	m_lovr(move(lovr))
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
	++ m_stat.numUniquePages;

	pair<Page, page_id_t> ret = getBackend()->newPage();

	return ret;
}

Page
TPIO2TxSession::readPage(page_id_t pgid)
{
	++ m_stat.nRead;

	page_id_t pgidOvr; ovr_status_t st;
	tie(pgidOvr, st) = m_lovr->searchOvr(pgid);

	Page pg = getBackend()->readPage(pgidOvr);
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
	if(m_aovr->tryCommit(tx->m_lovr))
	{
		return true;	
	}
	else
	{
		return false;	
	}
}


} // end of namespace ptnk
