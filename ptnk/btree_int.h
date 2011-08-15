#ifndef _ptnk_btree_int_h_
#define _ptnk_btree_int_h_

#include "page.h"
#include "btree.h"

namespace ptnk
{

class PageIO;
class Leaf;

//! structure holding data for b-tree leaf/node split
/*!
 *	The split is illustrated as follows:
 *
 *	before: [pgidSplit]
 *	after:  [pgidSplit] |split[0].key| [split[0].pgid] |split[1].key| [split[1].pgid]
 *
 *	[pgid] indicates a ptr to page, and |key| indicates a key fence, which equals to the first key value on the following page.
 */
struct btree_split_t
{
	struct split_page_t
	{
		BufferCRef key;
		page_id_t pgid;
	};

	enum
	{
		MAX_NUM_SPLIT = 4	
	};

	//! the page split
	page_id_t pgidSplit;

	//! if valid, target first key is changed
	Buffer keyNew;
	
	//! split pages
	split_page_t split[MAX_NUM_SPLIT];

	//! number of split pages
	unsigned int numSplit;

	//! target pgid to follow after split
	page_id_t pgidFollow;

	//! tmpbuf holding split[i].key datas (used by addSplit())
	Buffer tmpbuf;

	btree_split_t()
	{
		reset();	
	}

	void reset()
	{
		pgidSplit = pgidFollow = PGID_INVALID;
		numSplit = 0;
		tmpbuf.reset();
	}

	bool isValid() const
	{
		return pgidSplit != PGID_INVALID;
	}

	void addSplit(BufferCRef key, page_id_t pgid)
	{
		split[numSplit].key = tmpbuf.append(key);
		split[numSplit].pgid = pgid;
		++ numSplit;
	}

	void dump();
};

struct btree_cursor_t;

//! B-tree node
class Node : public Page
{
public:
	enum {
		TYPE = PT_NODE,
	};

	explicit Node(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == PT_NODE); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id)
	{
		initHdr(id, PT_NODE);
		// footer().numKeys = 0;
		// footer().sizeFree = BODY_SIZE - sizeof(footer_t) - sizeof(page_id_t) /* ptr_{-1} */;
	}

	void initBody(page_id_t pgidFirst);

	page_id_t query(const query_t& q) const;

	//! handle child leaf/node split
	/*!
	 *	@param [in,out] split
	 *		child split info.
	 *		this node's split data will overwritten here
	 *
	 *	@param [out] bOvr
	 *		set true if update has been handled by creating an ovr page
	 *
	 *	@return
	 *		Node that the child specified in _split_ belongs to
	 */
	Node handleChildSplit(btree_split_t* split, bool* bOvr, PageIO* pio);

	//! handle child leaf/node delete
	/*!
	 *	@param [out] pgidNextChild
	 *		page id of the child next to the target child to be removed. PGID_INVALID if the child to be removed is the right most child
	 *
	 *	@param [in] pgid
	 *		page id of the child node/leaf to be removed
	 *
	 *	@param [out] bOvr
	 *		set true if update has been handled by creating an ovr page
	 *	
	 *	@return
	 *		modified node, or invalid page when this node has been discarded as a result of del operation
	 */
	Node handleChildDel(page_id_t* pgidNextChild, page_id_t pgid, bool* bOvr, PageIO* pio);

	//! returns true if child node/leaf _split_ can be handled by this node without causing this node to split
	bool isRoomForSplitAvailable(const btree_split_t& split) const;

	Node handleChildSplitNoSelfSplit(btree_split_t* split, bool* bOvr, PageIO* pio);
	Node handleChildSplitSelfSplit(btree_split_t* split, bool* bOvr, PageIO* pio);

	bool contains_(page_id_t pgid)
	{
		if(pgid == ptrm1()) return true;
		unsigned int i, numKeys = footer().numKeys;
		for(i = 0; i < numKeys; ++ i)
		{
			if(pgid == ptr(i)) return true;
		}

		return false;
	}

	page_id_t ptrFront() const
	{
		return ptrm1();	
	}

	page_id_t ptrBack() const
	{
		return ptr(footer().numKeys);
	}

	page_id_t ptrBefore(page_id_t p) const;
	page_id_t ptrAfter(page_id_t p) const;

	void updateLinks_(mod_info_t* mod, PageIO* pio);
	void dump_(PageIO* pio = NULL) const;
	void dumpGraph_(FILE* fp, PageIO* pio = NULL) const;
	bool refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const;

private:
	Node();

	enum
	{
		NULL_TAG = 0xffff
	};

	// |*|*|k_0 size|k_0|*|k_1 size|k_1|*|k_2 size|k_2|...|k_{N-1}|*|k_N size|k_N| ..FREESPACE..
	//  | |              |              |                          |
	//  v > ptr_0        v              v                          v
	// ptr_-1            ptr_1          ptr_2          ...         ptr_N
	//
	// above ptr:key sets may not be in order
	//
	// ... |offset N|offset N-1|...|offset 0|footer| END
	// offset i points to ptr_i (in real order)

	struct footer_t {
		//! current number of keys = N
		uint16_t numKeys;

		//! free space size
		uint16_t sizeFree;
	} __attribute__((__packed__));

	footer_t& footer()
	{
		return *reinterpret_cast<footer_t*>(rawbody() + BODY_SIZE - sizeof(footer_t));
	}

	const footer_t& footer() const
	{
		return const_cast<Node*>(this)->footer();	
	}

	uint16_t& kp_offset(uint16_t i)
	{
		return *reinterpret_cast<uint16_t*>(rawbody() + BODY_SIZE - sizeof(footer_t) - (i+1)*sizeof(uint16_t));
	}

	uint16_t kp_offset(uint16_t i) const
	{
		return const_cast<Node*>(this)->kp_offset(i);
	}

	//! ptr_{-1}
	page_id_t& ptrm1()
	{
		return *reinterpret_cast<page_id_t*>(rawbody());	
	}

	//! ptr_{-1}
	page_id_t ptrm1() const
	{
		return *reinterpret_cast<const page_id_t*>(rawbody());	
	}

	typedef pair<BufferCRef, page_id_t> kp_t;
	typedef std::vector<kp_t> Vkp_t;
	kp_t kp(uint16_t i) const
	{
		pair<BufferCRef, page_id_t> ret;

		const char* kp = rawbody() + kp_offset(i);

		const uint16_t* ksize = reinterpret_cast<const uint16_t*>(kp + sizeof(page_id_t));
		if(PTNK_UNLIKELY(*ksize == NULL_TAG))
		{
			ret.first = BufferCRef::NULL_VAL;
		}
		else
		{
			ret.first = BufferCRef(ksize+1, *ksize);
		}

		ret.second = *reinterpret_cast<const page_id_t*>(kp);

		return ret;
	}

	page_id_t& ptr(uint16_t i)
	{
		return *reinterpret_cast<page_id_t*>(rawbody() + kp_offset(i));
	}

	page_id_t ptr(uint16_t i) const
	{
		return const_cast<Node*>(this)->ptr(i);
	}

	struct key_idx_comp
	{
		const Node* node;
		BufferCRef key;

		int operator()(int i) const
		{
			pair<BufferCRef, page_id_t> kp(node->kp(i));
			return bufcmp(kp.first, key);
		}
	};
	friend struct ptr_idx_comp;

	uint16_t addKP(BufferCRef key, page_id_t ptr)
	{
		uint16_t offset = BODY_SIZE - footer().sizeFree - sizeof(uint16_t)*footer().numKeys - sizeof(footer_t);
		char* kp = rawbody() + offset;
		*reinterpret_cast<page_id_t*>(kp) = ptr;
		uint16_t* ksize = reinterpret_cast<uint16_t*>(kp + sizeof(page_id_t));
		size_t kpackedsize = 0;
		if(PTNK_UNLIKELY(key.isNull()))
		{
			*ksize = NULL_TAG;
			kpackedsize = 0;
		}
		else
		{
			kpackedsize = *ksize = key.size();
			::memcpy(ksize+1, key.get(), kpackedsize);
		}

		footer().sizeFree -= sizeof(page_id_t) + sizeof(uint16_t)*2 + kpackedsize;
		++ footer().numKeys;

		return offset;
	}

	uint16_t addKP(const kp_t& kp)
	{
		return addKP(kp.first, kp.second);
	}
};
typedef std::vector<Node> VNode;

//! B tree leaf
class Leaf : public Page
{
public:
	enum {
		TYPE = PT_LEAF,
	};

	Leaf() { /* NOP */ }

	explicit Leaf(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == PT_LEAF); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id);
	void initBody();

	void query(btree_cursor_t* cursor, const query_t& q) const;
	void cursorGet(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, const btree_cursor_t& cursor) const;
	ssize_t cursorGetValue(BufferRef value, const btree_cursor_t& cursor) const;

	void cursorPut(btree_cursor_t* cur, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio);

	ssize_t get(BufferCRef key, BufferRef buf) const;

	//! put new kv
	/*!
	 *  @param [out] split
	 *		split info will be written in case of split
	 *
	 *	@param [out] mod
	 *		maybe filled by pio->modifyPage() internally issued
	 */
	void insert(BufferCRef key, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio, bool bAbortOnExisting = false);

	//! update value of the first key matching record
	void update(BufferCRef key, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio);

	bool cursorDelete(btree_cursor_t* cur, bool* bOvr, PageIO* pio);
	
	void dump_() const;
	void dumpGraph_(FILE* fp) const;

	BufferCRef keyFirst() const;

	int numKVs() const
	{
		return footer().numKVs;	
	}

private:
	typedef std::pair<BufferCRef, BufferCRef> KV;
	typedef std::vector<KV> VKV;

	//! check if key-value record (_key_, _value_) can be inserted w/o split
	bool isRoomForKVAvailable(BufferCRef key, BufferCRef value) const;

	//! check if value only record (_value_) can be inserted w/o split
	bool isRoomForVAvailable(BufferCRef value) const;

	void insertIdxNoExact(Leaf ovr, int new_i, BufferCRef key, BufferCRef value, btree_split_t* split, PageIO* pio);
	void updateIdx(Leaf ovr, int i, BufferCRef value, btree_split_t* split, PageIO* pio);

	void kvsCopy(VKV& kvs, int b, int e, BufferCRef* keyLast, char** tmpbuf) const;

	void kvsRef(VKV& kvs) const;
	void kvsCopyAll(VKV& kvs, char* tmpbuf) const;

	void kvsRefButIdx(VKV& kvs, int idx) const;
	void kvsCopyButIdx(VKV& kvs, int idx, char* tmpbuf) const;
	void kvsRefInsert(VKV& kvs, BufferCRef key, BufferCRef value, int new_i) const;
	void kvsCopyInsert(VKV& kvs, BufferCRef key, BufferCRef value, int new_i, char* tmpbuf) const;

	void doDefrag(const VKV& kvs, Leaf ovr, PageIO* pio);
	void doSplit(const VKV& kvs, Leaf ovr, size_t thresSplit, btree_split_t* split, PageIO* pio);

	enum
	{
		NULL_TAG = 0xffff,
		MAX_NUM_KVS = 255,

		VALUE_ONLY = 0x8000,
	};

	struct footer_t
	{
		uint8_t numKVs; //!< number of kv pairs in this leaf
		uint16_t sizeFree; //!< size of unclaimed space at last
	} __attribute__((__packed__));

	//! packed kv record in the leaf
	struct packedkv_t
	{
		uint16_t szKey;	
		uint16_t szValue;	
		char offset[];	
	} __attribute__((__packed__));

	//! packed value-only record in the leaf. Key is omitted as it is same as prev. record
	struct packedv_t
	{
		uint16_t szValue;	
		char offset[];	
	} __attribute__((__packed__));

	footer_t& footer()
	{
		return *reinterpret_cast<footer_t*>(rawbody() + BODY_SIZE - sizeof(footer_t));
	}

	const footer_t& footer() const
	{
		return const_cast<Leaf*>(this)->footer();	
	}

	uint16_t& kv_offset(int i)
	{
		return *reinterpret_cast<uint16_t*>(rawbody() + BODY_SIZE - sizeof(footer_t) - (i+1)*sizeof(uint16_t));
	}

	uint16_t kv_offset(int i) const
	{
		return const_cast<Leaf*>(this)->kv_offset(i);	
	}

	BufferCRef getV(int i) const
	{
		uint16_t kvo = kv_offset(i);
		if(kvo & VALUE_ONLY)
		{
			const packedv_t* v = reinterpret_cast<const packedv_t*>(rawbody() + kvo - VALUE_ONLY);
			if(PTNK_UNLIKELY(v->szValue == NULL_TAG))
			{
				return BufferCRef::NULL_VAL;
			}
			else
			{
				return BufferCRef(v->offset, v->szValue);	
			}
		}
		else
		{
			const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
			if(PTNK_UNLIKELY(kv->szValue == NULL_TAG))
			{
				return BufferCRef::NULL_VAL;
			}
			else
			{
				size_t packedszKey;
				if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
				{
					packedszKey = 0;
				}
				else
				{
					packedszKey = kv->szKey;
				}
				
				return BufferCRef(kv->offset + packedszKey, kv->szValue);
			}	
		}
	}

	size_t offsetFree() const
	{
		return BODY_SIZE - footer().sizeFree - sizeof(uint16_t)*footer().numKVs - sizeof(footer_t);
	}

	uint16_t addKV(BufferCRef key, BufferCRef value)
	{
		PTNK_ASSERT(key.isValid());
		PTNK_ASSERT(value.isValid());

		size_t offset = offsetFree();
		packedkv_t* kv = reinterpret_cast<packedkv_t*>(rawbody() + offset);

		size_t ksize_packed, vsize_packed;
		if(PTNK_UNLIKELY(key.isNull()))
		{
			kv->szKey = NULL_TAG;
			ksize_packed = 0;
		}
		else
		{
			ksize_packed = kv->szKey = static_cast<size_t>(key.size());
			::memcpy(kv->offset, key.get(), ksize_packed);
		}
		
		if(PTNK_UNLIKELY(value.isNull()))
		{
			kv->szValue = NULL_TAG;
			vsize_packed = 0;
		}
		else
		{
			vsize_packed = kv->szValue = static_cast<size_t>(value.size());
			::memcpy(kv->offset + ksize_packed, value.get(), vsize_packed);
		}
		footer().sizeFree -= sizeof(uint16_t)*3 + ksize_packed + vsize_packed;
		++ footer().numKVs;

		return offset;
	}

	uint16_t addV(BufferCRef value)
	{
		PTNK_ASSERT(value.isValid());

		size_t offset = offsetFree();
		packedv_t* v = reinterpret_cast<packedv_t*>(rawbody() + offset);

		size_t vsize_packed = 0;
		if(PTNK_UNLIKELY(value.isNull()))
		{
			v->szValue = NULL_TAG;
		}
		else
		{
			vsize_packed = v->szValue = static_cast<size_t>(value.size());
			::memcpy(v->offset, value.get(), vsize_packed);
		}

		footer().sizeFree -= sizeof(uint16_t)*2 + vsize_packed;
		++ footer().numKVs;

		return offset;
	}

	uint16_t addKV(const KV& kv)
	{
		return addKV(kv.first, kv.second);	
	}

	//! perform binary search and find the smallest idx where new record with _key_ can be inserted without breaking order
	/*!
	 *	@return
	 *		ret.first -> the smallest idx where new record with _key_ can be inserted without breaking order
	 *		ret.second -> record[ret.first] has _key_
	 */
	pair<int, bool> idx_lower_bound(int b, int e, BufferCRef key) const;

	//! perform binary search and find the largest idx where new record with _key_ can be inserted without breaking order
	/*!
	 *	@return
	 *		ret.first -> the largest idx where new record with _key_ can be inserted without breaking order
	 *		ret.second -> record[ret.first] has _key_
	 */
	pair<int, bool> idx_upper_bound(int b, int e, BufferCRef key) const;
};

class DupKeyNode : public Page
{
public:
	enum { TYPE = PT_DUPKEYNODE, };

	DupKeyNode() { /* NOP */ }

	explicit DupKeyNode(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == TYPE); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id);
	void initBody(BufferCRef key);

	void addFirstChild(page_id_t pgidChild)
	{
		e(0).ptr = pgidChild;
		e(0).sizeFree = MOSTFREE_TAG;
		header().nPtr = 1;
	}

	void insert(BufferCRef value, bool *bNotifyOldLink, PageIO* pio);
	// bool update(btree_cursor_t* cur, BufferCRef value, mod_info_t* mod, PageIO* pio);

	BufferCRef key() const;

	uint16_t ptrsFree() const
	{
		const header_t& h = header();
		return h.nPtrMax - h.nPtr;
	}

	void updateLinks_(mod_info_t* mod, PageIO* pio);
	void dump_(PageIO* pio = NULL) const;
	void dumpGraph_(FILE* fp, PageIO* pio = NULL) const;
	bool refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const;

	page_id_t ptrFront() const
	{
		return e(0).ptr;	
	}
	page_id_t ptrBack() const
	{
		return e(header().nPtr-1).ptr;	
	}

	page_id_t ptrBefore(page_id_t p) const;
	page_id_t ptrAfter(page_id_t p) const;

private:
	bool insertR(BufferCRef value, bool *bNotifyOldLink, PageIO* pio);
	// bool updateR(btree_cursor_t* cur, BufferCRef value, mod_info_t* mod, PageIO* pio);

	enum
	{
		NULL_TAG=0xffff,
		NOKEY_TAG=0xfffe,

		MOSTFREE_TAG = 0xffff,
	};

	struct header_t
	{
		uint16_t szKey;
		uint16_t nPtr;
		uint16_t nPtrMax;
		uint8_t lvl;
	} __attribute__((__packed__));

	struct entry_t
	{
		page_id_t ptr;
		uint16_t sizeFree;
	} __attribute__((__packed__));

	header_t& header()
	{
		return *reinterpret_cast<header_t*>(rawbody());
	}

	const header_t& header() const
	{
		return *reinterpret_cast<const header_t*>(rawbody());
	}

	entry_t& e(int i)
	{
		return *reinterpret_cast<entry_t*>(rawbody() + sizeof(header_t) + sizeof(struct entry_t)*i);
	}

	const entry_t& e(int i) const
	{
		return const_cast<DupKeyNode*>(this)->e(i);	
	}

	void removeKey();
};
typedef std::vector<DupKeyNode> VDupKeyNode;

//! special leaf with same-key records
/*!
 *	FIXME: null value should be kept by a flag / not NULL_TAG, as if NULL_TAG checks are inefficient
 */
class DupKeyLeaf : public Page
{
public:
	enum { TYPE = PT_DUPKEYLEAF, };

	DupKeyLeaf() { /* NOP */ }

	explicit DupKeyLeaf(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == PT_DUPKEYLEAF); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id);
	void initBody(BufferCRef key);

	//! add record _value_
	void addValue(BufferCRef value);

	void dump_(PageIO* pio) const;
	void dumpGraph_(FILE* fp, PageIO* pio) const;

	uint16_t numVs() const
	{
		return footer().numVs;	
	}

	uint16_t sizeFree() const
	{
		return footer().sizeFree;	
	}

	BufferCRef key() const
	{
		uint16_t packedszKey = footer().szKey;
		if(PTNK_UNLIKELY(packedszKey == NULL_TAG))
		{
			return BufferCRef::NULL_VAL;	
		}
		else if(PTNK_UNLIKELY(packedszKey == NOKEY_TAG))
		{
			return BufferCRef();	
		}
		else
		{
			return BufferCRef(rawbody() + BODY_SIZE - sizeof(footer_t) - packedszKey, packedszKey);
		}
	}

	BufferCRef vByOffset(int offset) const
	{
		PTNK_ASSERT(0 <= offset);
		PTNK_ASSERT((size_t)offset < BODY_SIZE - sizeof(footer_t));
			
		const char* p = rawbody() + offset;
		uint16_t packedszValue = *reinterpret_cast<const uint16_t*>(p);

		if(packedszValue == NULL_TAG)
		{
			return BufferCRef::NULL_VAL;
		}
		else
		{
			return BufferCRef(p + sizeof(uint16_t), packedszValue);
		}
	}

	void offsetPrev(int* offset) const
	{
		if(*offset <= 0)
		{
			*offset = -1;
			return;
		}

		// this is very inefficient but no other way

		// scan values from front until reach offset
		int o = 0, prev_offset = 0;
		do
		{
			const char* p = rawbody() + o;
			uint16_t packedszValue = *reinterpret_cast<const uint16_t*>(p);
			PTNK_ASSERT(packedszValue != SENTINEL);

			if(PTNK_UNLIKELY(packedszValue == NULL_TAG)) packedszValue = 0;

			prev_offset = o;
			o += sizeof(uint16_t) + packedszValue;
		}
		while(o < *offset);

		*offset = prev_offset;
	}

	BufferCRef vByOffsetAndNext(int* offset) const
	{
		PTNK_ASSERT(0 <= *offset);
		PTNK_ASSERT((size_t)*offset < BODY_SIZE - sizeof(footer_t));

		const char* p = rawbody() + *offset;
		uint16_t packedszValue = *reinterpret_cast<const uint16_t*>(p);
		BufferCRef ret(false);
		if(packedszValue == NULL_TAG)
		{
			*offset += sizeof(uint16_t);
			ret = BufferCRef::NULL_VAL;
		}
		else
		{
			*offset += sizeof(uint16_t) + packedszValue;
			ret = BufferCRef(p + sizeof(uint16_t), packedszValue);
		}

		if(*reinterpret_cast<const uint16_t*>(rawbody() + *offset) == SENTINEL)
		{
			*offset = -1;
		}
		return ret;
	}

	int offsetIdx(int idx) const
	{
		int offset = 0;
		for(int i = 0; i < idx; ++ i)
		{
			const char* p = rawbody() + offset;
			uint16_t packedszValue = *reinterpret_cast<const uint16_t*>(p);
			if(PTNK_UNLIKELY(packedszValue == NULL_TAG)) packedszValue = 0;

			offset += sizeof(uint16_t) + packedszValue;
		}

		return offset;
	}

	int offsetBack() const
	{
		return offsetIdx(footer().numVs - 1);	
	}

	BufferCRef v(int idx) const
	{
		return vByOffset(offsetIdx(idx));
	}

	bool insert(BufferCRef value, bool* bNotifyOldLink, PageIO* pio);
	bool update(BufferCRef value, bool* bNotifyOldLink, PageIO* pio);

	bool popValue(mod_info_t* mod, PageIO* pio);

	DupKeyNode makeTree(bool* bNotifyOldLink, PageIO* pio);

private:
	enum
	{
		SENTINEL = 0xfffd,
		NOKEY_TAG = 0xfffe,
		NULL_TAG = 0xffff,

		F_PSEUDOKEY = 0x8000,
	};

	struct footer_t
	{
		uint16_t numVs;
		uint16_t szKey;
		uint16_t sizeFree;
	} __attribute__((__packed__));
	
	footer_t& footer()
	{
		return *reinterpret_cast<footer_t*>(rawbody() + BODY_SIZE - sizeof(footer_t));
	}

	const footer_t& footer() const
	{
		return const_cast<DupKeyLeaf*>(this)->footer();	
	}

	void removeKey();
};

struct btree_cursor_t
{
	//! B-tree node page stack
	VNode nodes;

	//! B-tree leaf page (maybe Leaf / DupKeyNode / DupKeyLeaf)
	Page leaf;
	
	//! record idx inside Leaf _leaf_
	int idx;

	//! DupKey tree nodes stack
	VDupKeyNode dknodes;

	//! DupKey tree leaf
	DupKeyLeaf dkleaf;

	//! DupKey tree leaf offset
	int dloffset;

	enum
	{
		PREV_LEAF = -1,
		NO_MATCH = -2,
		SEE_DUPKEY_OFFSET = -3,
	};

	btree_cursor_t()
	:	idx(NO_MATCH), dloffset(0)
	{
		nodes.reserve(8);
		dknodes.reserve(8);
	}

	void reset()
	{
		nodes.clear();
		leaf = Leaf();
		idx = NO_MATCH;
		dknodes.clear();
		dkleaf = DupKeyLeaf();
		dloffset = 0;
	}

	bool isValid() const
	{
		return leaf.isValid() && idx != NO_MATCH;
	}
};

} // end of namespace ptnk

#endif // _ptnk_btree_int_h_
