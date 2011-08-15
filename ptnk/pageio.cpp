#include "pageio.h"

#include <stdio.h>

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

void
mod_info_t::dump() const
{
	std::cout << "** mod_info_t dump" << std::endl;	
	std::cout << "idOrig:\t" << pgid2str(idOrig) << std::endl;
	std::cout << "idOvr:\t" << pgid2str(idOvr) << std::endl;
}

PageIO::~PageIO()
{
	/* NOP */
}

Page
PageIO::modifyPage(const Page& page, mod_info_t* mod)
{
	Page& ret = const_cast<Page&>(page);
	ret.setMutable();
	mod->reset();

	return ret;
}

void
PageIO::discardPage(page_id_t pgid, mod_info_t* mod)
{
	if(mod)
	{
		mod->idOrig = pgid;
		mod->idOvr = PGID_INVALID;
	}
}

void
PageIO::syncRange(page_id_t pgidStart, page_id_t pgidEnd)
{
	page_id_t pgidLast = getLastPgId();
	if(pgidEnd > pgidLast)
	{
		pgidEnd = pgidLast;	
	}

	for(page_id_t pgid = pgidStart; pgidStart <= pgidEnd; ++ pgidStart)
	{
		sync(pgid);	
	}
}

page_id_t
PageIO::getFirstPgId() const
{
	return 0;
}

local_pgid_t
PageIO::getPartLastLocalPgId(part_id_t) const
{
	// fall back if no part support
	return PGID_LOCALID(getLastPgId());
}

void
PageIO::newPart(bool bForce)
{
	// do nothing if no part support
	/* NOP */
}

bool
PageIO::needInit() const
{
	return true;
}

void
PageIO::dumpStat() const
{
	std::cout << "default PageIO::dumpStat !! override me !!" << std::endl;
}

void
PageIO::notifyPageWOldLink(page_id_t id, page_id_t idDep)
{
	/* NOP */
}

page_id_t
PageIO::updateLink(page_id_t idOld)
{
	// no update by default
	return idOld;
}

void
PageIO::discardOldPages(page_id_t threshold)
{
	std::cout << "PageIO::discardOldPages called but not implemented. threshold : " << pgid2str(threshold) << std::endl;
}

} // end of namespace ptnk
