#include "page.h"

#include "pageio.h" // for DebugPage::set

#include <iostream>

namespace ptnk
{

std::string
pgid2str(page_id_t pgid)
{
	if(pgid == PGID_INVALID)
	{
		return "INVALID";
	}

	char buf[20];
	sprintf(buf, "%x:%03llu", (unsigned int)PGID_PARTID(pgid), PGID_LOCALID(pgid));
	
	return buf;
}

Page::dyndispatcher_t* Page::ms_dyndispatch[PT_MAX+1];

void
Page::dumpHeader() const
{
	std::cout << "Page [id:" << pgid2str(hdr()->id) << " -> ovr:" << pgid2str(hdr()->idOvrTgt) << "] type: " << (uint16_t)(hdr()->type) << " txid: " << hdr()->txid << " flags: " << (uint16_t)hdr()->flags << std::endl;
}

void
Page::dump(PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];
	if(!table || !table->dump)
	{
		dumpHeader();
		std::cout << "** unknown pagetype" << std::endl;
	}
	else
	{
		table->dump(*this, pio);
	}
}

void
Page::dumpGraph(FILE* fp, PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];
	if(!table || !table->dumpGraph)
	{
		dumpHeader();
		std::cout << "** unknown pagetype" << std::endl;
	}
	else
	{
		table->dumpGraph(*this, fp, pio);
	}
}

bool
Page::refreshAllLeafPages(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];

	PTNK_CHECK(table);
	PTNK_CHECK(table->refreshAllLeafPages);

	return table->refreshAllLeafPages(*this, cursor, threshold, numPages, pgidDep, pio);
}

void
DebugPage::set(char c, bool* bOvr, PageIO* pio)
{
	DebugPage ovr(pio->modifyPage(*this, bOvr));

	*ovr.rawbody() = c;

	pio->sync(ovr);
}

} // end of namespace ptnk
