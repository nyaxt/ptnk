#ifndef _ptnk_pol_h_
#define _ptnk_pol_h_

#include "types.h"
#include "buffer.h"

#include <boost/foreach.hpp>

namespace ptnk
{

class PagesOldLink
{
public:
	PagesOldLink();
	~PagesOldLink();

	void add(page_id_t pgid)
	{
		m_impl.insert(pgid);
	}

	void merge(const PagesOldLink& o);

	bool contains(page_id_t pgid) const
	{
		return m_impl.find(pgid) != m_impl.end();	
	}

	void clear()
	{
		m_impl.clear();	
	}

	size_t size()
	{
		return m_impl.size();	
	}

	template<typename T>
	void dump(T& tgt)
	{
		size_t count = m_impl.size();
		tgt.write(BufferCRef(&count, sizeof(size_t)));

		BOOST_FOREACH(page_id_t pgid, m_impl)
		{
			BufferCRef buf(&pgid, sizeof(page_id_t));
			tgt.write(buf);
		}
	}

	template<typename T>
	void restore(T& tgt)
	{
		size_t count;
		tgt.popFrontTo(&count, sizeof(size_t));

		for(size_t i = 0; i < count; ++ i)
		{
			page_id_t pgid;
			tgt.popFrontTo(&pgid, sizeof(page_id_t));
			
			add(pgid);
		}
	}

	void dumpStr(std::ostream& s) const;

private:
	//! pages with links to already overrode pages
	Spage_id_t m_impl;

	friend class TPIO;
};
inline
std::ostream& operator<<(std::ostream& s, const PagesOldLink& o)
{ o.dumpStr(s); return s; }

} // end of namespace ptnk

#endif // _ptnk_pol_h_
