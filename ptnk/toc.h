#ifndef _ptnk_toc_h_
#define _ptnk_toc_h_

#include "buffer.h"
#include "page.h"

namespace ptnk
{

//! table pgid offset cache
/*!
 *	In the OverviewPage::set/getTableRoot impls, O(N) bufcmps are required to find table rootpgid.
 *	This cache eliminates the bufcmps by storing the offset of the tables rootpgid.
 */
class TableOffCache
{
public:
	TableOffCache(BufferCRef tableid)
	:	m_verLayout(PGID_INVALID),
		m_offset(0)
	{
		m_tableid = tableid;
	}

	~TableOffCache()
	{ /* NOP */ } 

	// ------ accessor methods ------

	BufferCRef getTableId() const
	{
		return m_tableid.rref();	
	}

private:
	//! table id
	Buffer m_tableid;	

	//! the OverviewPage layout version number
	/*!
	 *	if this is different, the cache would be invalid.
	 */
	page_id_t m_verLayout;

	//! the pgid of the table root node is stored at offsetEntries() + m_offset
	uint16_t m_offset;

	friend class OverviewPage;
};

} // end of namespace ptnk


#endif // _ptnk_toc_h_
