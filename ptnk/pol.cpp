#include "pol.h"
#include "page.h"

namespace ptnk
{

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

void
PagesOldLink::dumpStr(std::ostream& s) const
{
	s << "<#PagesOldLink: [";
	for(page_id_t pgid: m_impl)
	{
		s << pgid2str(pgid) << ", ";
	}
	s << "]>";
}

} // end of namespace ptnk
