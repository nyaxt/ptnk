#ifndef _ptnk_query_h_
#define _ptnk_query_h_

#include "buffer.h"
#include "types.h"

namespace ptnk
{

//! struct holding data for record search query
/*!
 *	FIXME: this is not the right place to define this, but any other place?
 */
struct query_t
{
	BufferCRef key;
	query_type_t type;

	bool isValid() const
	{
		return (type & F_NOSEARCH) || key.isValid();
	}
};

} // end of namespace ptnk

#endif // _ptnk_query_h_
