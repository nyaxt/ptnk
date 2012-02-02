#ifndef _ptnk_btree_h_
#define _ptnk_btree_h_

#include "query.h"
#include "page.h"

namespace ptnk
{

/*!
 * @file btree.h
 * @brief B+ like tree operations definitions
 *
 * @description
 *   This file contains operations for B+ tree like tree, used as primary
 *   (logical) data structure for ptnk. All records (the key-value pairs)
 *   are kept in leafs, and nodes only contains keys sorely for index
 *   purpose.
 *
 *   The main difference to the well-known B+ tree is that this tree
 *   does NOT feature linked lists between leaf nodes.
 *   The linked list is known to be useful for accelerating sequential
 *   table read operations, but it will cause all leafs to be dependent of
 *   each others' references. This will cause all leafs to be re-written
 *   on a leaf's change when using copy-on-write strategy.
 *
 * @note
 *	 consult btree_int.h for Node/Leaf class defs etc.	
 */

class PageIO;

struct btree_cursor_t;

//! create new btree and return root node page id
page_id_t btree_init(PageIO* pio);

//! get _value_ corresponding to specified _key_ in the btree
/*!
 *	@param [in] idRoot
 *		root node page id, returned from btree_init()
 *
 *	@param [in] key
 *		look up key
 *
 *	@param [out] value
 *		the buffer where the value will be stored if found
 *
 *	@param [in] pio
 *		PageIO used for lookup
 *
 *	@return
 *		value size, -1 if value not found
 */
ssize_t btree_get(page_id_t idRoot, BufferCRef key, BufferRef value, PageIO* pio); 

//! associate _value_ to _key_ in the btree
/*!
 *	@param [in] idRot
 *		root node page id, returned from btree_init() / previous call to btree_put()
 *
 *	@param [in] key
 *		key to associate _value_ to
 *
 *	@param [in] value
 *		the value
 *
 *	@param [in] mode
 *		either PUT_UPDATE / PUT_INSERT is accepted
 *		PUT_UPDATE: overwrite previous value associated to the _key_
 *		PUT_INSERT: create another entry without overwriting previous value. all values associated to the _key_ can be retrieved through cursor apis
 *
 *	@param [in] pio
 *		PageIO used for modification
 *
 *	@return
 *		page id of the new root (this may/may not be same as _idRoot_)
 */
page_id_t btree_put(page_id_t idRoot, BufferCRef key, BufferCRef value, put_mode_t mode, page_id_t pgidDep, PageIO* pio);

//! delete a first kv record with specified key
/*!
 *	@param [in] idRoot
 *		root node page id, returned from btree_init()
 *
 *	@param [in] key
 *		look up key
 *
 *	@param [in] pio
 *		PageIO used to access BTree nodes/leaves
 *
 *	@return
 *		new root node page id
 */
page_id_t btree_del(page_id_t idRoot, BufferCRef key, PageIO* pio);

//! create a new btree cursor object
/*!
 *	@sa btree_cursor_delete
 */
btree_cursor_t* btree_cursor_new();

//! delete a btree cursor _cur_
void btree_cursor_delete(btree_cursor_t* cur);

//! get root node of the btree which the record pointed by _cur_ is contained
page_id_t btree_cursor_root(btree_cursor_t* cur);

void btree_query(btree_cursor_t* cur, page_id_t idRoot, const query_t& query, PageIO* pio);

inline
void btree_cursor_front(btree_cursor_t* cur, page_id_t idRoot, PageIO* pio)
{
	query_t q; q.type = FRONT;
	btree_query(cur, idRoot, q, pio);
}

inline
void btree_cursor_back(btree_cursor_t* cur, page_id_t idRoot, PageIO* pio)
{
	query_t q; q.type = BACK;
	btree_query(cur, idRoot, q, pio);
}

void btree_cursor_get(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, btree_cursor_t* cur, PageIO* pio);
page_id_t btree_cursor_put(btree_cursor_t* cur, BufferCRef value, PageIO* pio);

//! delete active record pointed by _cur_
/*!
 *	@return
 *		<true if cursor is still valid, new btree root node pgid>
 */
pair<bool, page_id_t> btree_cursor_del(btree_cursor_t* cur, PageIO* pio);
bool btree_cursor_next(btree_cursor_t* cur, PageIO* pio, bool bNormalizeOnly = false);
bool btree_cursor_prev(btree_cursor_t* cur, PageIO* pio);
bool btree_cursor_valid(btree_cursor_t* cur);
void btree_cursor_dump(btree_cursor_t* cur, PageIO* pio);

#ifndef PTNK_NO_CURSOR_WRAP

class btree_cursor_wrap : noncopyable
{
public:
	btree_cursor_wrap()
	:	m_impl(btree_cursor_new())
	{ /* NOP */ }

	~btree_cursor_wrap() { btree_cursor_delete(m_impl); }

	btree_cursor_t* get() { return m_impl; }

private:
	btree_cursor_t* m_impl;
};

#endif // PTNK_NO_CURSOR_WRAP

} // end of namespace ptnk

#endif // _ptnk_btree_h_
