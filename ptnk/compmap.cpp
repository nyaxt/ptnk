#include "compmap.h"
#include "pageio.h"

namespace ptnk
{

void
CompMapPage::initBody(uint64_t offsetOrig)
{
	bodyhdr().offsetOrig = offsetOrig;	
	bodyhdr().offsetComp = ~0; // unknown at this point

	::memset(rawbody() + sizeof(body_hdr_t), 0, BV_LENGTH_BYTES);
}

size_t
CompMapPage::calcOffset(local_pgid_t pgidOrig)
{
	size_t off = pgidOrig - bodyhdr().offsetOrig;
	PTNK_ASSERT(off < BV_LENGTH);

	return off;
}

page_id_t
CompMapPage::translate(page_id_t pgidOrig)
{
	return bodyhdr().offsetComp + bv().popcnt_to(calcOffset(pgidOrig));
}

void
CompMapPage::mark(local_pgid_t pgidOrig)
{
	bv().set(calcOffset(pgidOrig));
}

size_t
CompMapPage::countMarks()
{
	return bv().popcnt();
}

CompMap::CompMap()
:	m_pgidoffCompMap(0), m_numOrigPages(0)
{
	/* NOP */
}

page_id_t
CompMap::initPages(local_pgid_t numOrigPages, PageIO* pio)
{
	m_pgidoffCompMap = pio->getLastPgId() + 1;
	page_id_t pgid = m_pgidoffCompMap;
	for(local_pgid_t pgidoffOrig = 0; pgidoffOrig < numOrigPages; pgidoffOrig += CompMapPage::BV_LENGTH, ++ pgid)
	{
		CompMapPage cmp(pio->newInitPage<CompMapPage>());
		PTNK_ASSERT_CMNT(cmp.pageId() == pgid, "id of pages alloced not consecutive, maybe other alloc happening duing the process?");

		cmp.initBody(pgidoffOrig);
	}
	m_numOrigPages = pgid - m_pgidoffCompMap;

	return pgid;
}

inline
page_id_t
CompMap::calcPgidCompMap(local_pgid_t pgidOrig)
{
	return m_pgidoffCompMap + pgidOrig / CompMapPage::BV_LENGTH;
}

local_pgid_t
CompMap::translate(local_pgid_t pgidOrig, PageIO* pio)
{
	page_id_t pgidCompMap = calcPgidCompMap(pgidOrig);
	
	CompMapPage cmp(pio->readPage(pgidCompMap));
	return cmp.translate(pgidOrig);
}

void
CompMap::mark(local_pgid_t pgidOrig, PageIO* pio)
{
	page_id_t pgidCompMap = calcPgidCompMap(pgidOrig); 
	
	CompMapPage cmp(pio->readPage(pgidCompMap));
	cmp.mark(pgidOrig);
}

void
CompMap::fillOffsetComps(PageIO* pio)
{
	uint64_t offsetComp = 0;
	for(unsigned int i = 0; i < m_numOrigPages; ++ i)
	{
		CompMapPage cmp(pio->readPage(m_pgidoffCompMap + i));
		cmp.setOffsetComp(offsetComp);	
		offsetComp += cmp.countMarks();
	}
}

} // end of namespace ptnk
