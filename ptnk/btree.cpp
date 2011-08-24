#include "btree.h"
#include "btree_int.h"
#include "pageio.h"

#include <stdio.h>
#include <boost/tuple/tuple.hpp>

namespace ptnk
{

static bool btree_cursor_prevleaf(btree_cursor_t* cur, PageIO* pio);
static bool btree_cursor_nextleaf(btree_cursor_t* cur, PageIO* pio);
static bool dktree_cursor_nextdkleaf(btree_cursor_t* cur, PageIO* pio);
static bool dktree_cursor_prevdkleaf(btree_cursor_t* cur, PageIO* pio);
static void dktree_cursor_front(btree_cursor_t* cur, PageIO* pio);
static void dktree_cursor_back(btree_cursor_t* cur, PageIO* pio);
static page_id_t btree_propagate(btree_split_t& split, bool bPrevWasOvr, btree_cursor_t* cur, PageIO* pio);

namespace 
{

void node_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ Node(pg).updateLinks_(mod, pio); }

void node_dump(const Page& pg, PageIO* pio)
{ Node(pg).dump_(pio); }

void node_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ Node(pg).dumpGraph_(fp, pio); }

bool node_refreshAllLeafPages(const Page& pg, void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio)
{ return Node(pg).refreshAllLeafPages_(cursor, threshold, numPages, pgidDep, pio); }

static Page::dyndispatcher_t g_node_handlers = 
{
	node_updateLinks,
	node_dump,
	node_dumpGraph,
	node_refreshAllLeafPages
};

Page::register_dyndispatcher g_node_reg(PT_NODE, &g_node_handlers);

size_t
packedsize(BufferCRef data)
{
	PTNK_ASSERT(data.isValid());

	if(data.isNull())
	{
		return 0;
	}
	else
	{
		return static_cast<size_t>(data.size());	
	}
}

} // end of anonymous namespace

void
btree_split_t::dump()
{
	std::cout << "* btree_split_t ******" << std::endl;
	std::cout << " - pgidSplit: " << pgid2str(pgidSplit) << std::endl;
	std::cout << " - numSplit: " << numSplit << std::endl;
	for(unsigned int i = 0; i < numSplit; ++ i)
	{
		std::cout << " key [" << i << "]: " << split[i].key.inspect() << std::endl;	
		std::cout << " pgid[" << i << "]: " << split[i].pgid << std::endl;	
	}
}

void
Node::initBody(page_id_t pgidFirst)
{
	footer().numKeys = 0;
	footer().sizeFree = BODY_SIZE - sizeof(footer_t) - sizeof(page_id_t) /* ptr_{-1} */;
	ptrm1() = pgidFirst;
}

page_id_t
Node::query(const query_t& query) const
{
	if(query.type & F_NOSEARCH)
	{
		switch(query.type)
		{
		case FRONT:
			return ptrm1();

		case BACK:
			if(footer().numKeys > 0)
			{
				return ptr(footer().numKeys - 1);
			}
			else
			{
				return ptrm1();	
			}

		default:
			PTNK_THROW_RUNTIME_ERR("unknown query type (w/ F_NOSEARCH)");
		}
	}

	key_idx_comp comp = {this, query.key};
	int i = idx_lower_bound(0, footer().numKeys, comp);
	if(query.type == BEFORE)
	{
		-- i;	
	}
	else
	{
		BufferCRef key; page_id_t _;
		boost::tie(key, _) = kp(i);
		if(! bufeq(key, query.key)) -- i;
	}

	if(i >= 0)
	{
		return ptr(i);
	}
	else
	{
		return ptrm1();	
	}
}

Node
Node::handleChildSplit(btree_split_t* split, bool* bOvr, PageIO* pio)
{
	PTNK_ASSERT(split);
	PTNK_ASSERT(split->isValid());
	PTNK_ASSERT(bOvr);

	if(PTNK_UNLIKELY(split->keyNew.isValid()))
	{
		// handling keyNew involves Node defrag
		return handleChildSplitSelfSplit(split, 0, bOvr, pio);
	}

	if(PTNK_LIKELY(isRoomForSplitAvailable(*split)))
	{
		return handleChildSplitNoSelfSplit(split, bOvr, pio);
	}
	else
	{
		return handleChildSplitSelfSplit(split, BODY_SIZE/2, bOvr, pio);
	}
}

bool
Node::isRoomForSplitAvailable(const btree_split_t& split) const
{
	int sizeFree = footer().sizeFree;
	if(split.keyNew.isValid() && ! split.keyNew.isNull())
	{
		sizeFree -= split.keyNew.valsize();	
	}
	for(unsigned int i = 0; i < split.numSplit; ++ i)
	{
		sizeFree -= packedsize(split.split[i].key) + sizeof(page_id_t) + sizeof(uint16_t)*2;
	}
	return sizeFree >= 0;
}

Node
Node::handleChildSplitNoSelfSplit(btree_split_t* split, bool* bOvr, PageIO* pio)
{
	Node ovr(pio->modifyPage(*this, bOvr));

	const page_id_t pgidSplit = split->pgidSplit;
	const int numSplit = split->numSplit;

	// find the old child page
	page_id_t ptr_i1 = ptrm1();
	int i, numKeys = footer().numKeys;
	for(i = 0; i <= numKeys; ++ i)
	{
		if(pgidSplit == ptr_i1)
		{
			break;
		}

		ptr_i1 = ptr(i);
	}
	PTNK_ASSERT(i <= numKeys)
	{
		std::cout << "i: " << i << " numKeys: " << numKeys << std::endl;
		dump_();
		split->dump();
	}

	// shift kps after
	for(int j = numKeys - 1; j >= i; -- j)
	{
		ovr.kp_offset(j + numSplit) = ovr.kp_offset(j);
	}
	for(int is = 0; is < numSplit; ++ is)
	{
		ovr.kp_offset(i + is) = ovr.addKP(split->split[is].key, split->split[is].pgid);
	}

	pio->sync(ovr);

	split->reset();
	split->pgidFollow = pageOrigId();

	return ovr;
}

Node
Node::handleChildSplitSelfSplit(btree_split_t* split, size_t thresSplit, bool* bOvr, PageIO* pio)
{
	PTNK_ASSERT(footer().numKeys > 2);

	const page_id_t ptrm1_ = ptrm1();
	std::vector<kp_t> kps; kps.reserve(512);
	int iFollow = -1; // split->pgidFollow idx in kps

	const page_id_t pgidSplit = split->pgidSplit;
	const int numKeys = footer().numKeys;

	Node ovr(pio->modifyPage(*this, bOvr));
	if(pageId() != ovr.pageId())
	{
		// if operation result is to be saved on different page,
		// the KPs stored on the old page is valid throughout the op

		int i = 0;
		if(pgidSplit != ptrm1_)
		{
			for(; i < numKeys; ++ i)
			{
				kp_t e = kp(i);
				if(e.second == pgidSplit)
				{
					if(split->keyNew.isValid())
					{
						kps.push_back(make_pair(split->keyNew.rref(), e.second));
					}
					else
					{
						kps.push_back(e);
					}

					++ i;
					break;
				}
				else
				{
					kps.push_back(e);
				}
			}
		}
		iFollow = i;
		for(unsigned int is = 0; is < split->numSplit; ++ is)
		{
			if(split->split[is].pgid == split->pgidFollow)
			{
				iFollow = i + is + 1;
			}

			const BufferCRef& key = split->split[is].key;

			char* kt = NULL;
			if(! key.isNull())
			{
				kt = (char*)::alloca(key.size());
				::memcpy(kt, key.get(), key.size());
			}
			kps.push_back(make_pair(BufferCRef(kt, key.size()), split->split[is].pgid));
		}
		for(; i < numKeys; ++ i)
		{
			kps.push_back(kp(i));
		}
	}
	else
	{
		// if operation is done in-place, we need to keep copy of old kps
		// the copy is allocated from the stack by alloca
#define ALLOC_COPY_OLDKP(i) \
		kp_t e = kp(i); \
		char *kt = NULL; \
		if(! e.first.isNull()) { kt = (char*)::alloca(e.first.size()); ::memcpy(kt, e.first.get(), e.first.size()); e.first = BufferCRef(kt, e.first.size()); }

		int i = 0;
		if(pgidSplit != ptrm1_)
		{
			for(; i < numKeys; ++ i)
			{
				ALLOC_COPY_OLDKP(i);
				if(e.second == pgidSplit)
				{
					if(split->keyNew.isValid())
					{
						kps.push_back(make_pair(split->keyNew.rref(), e.second));
					}
					else
					{
						kps.push_back(e);
					}

					++ i;
					break;
				}
				else
				{
					kps.push_back(e);
				}
			}
		}
		iFollow = i;
		for(unsigned int is = 0; is < split->numSplit; ++ is)
		{
			if(split->split[is].pgid == split->pgidFollow)
			{
				iFollow = i + is + 1;
			}

			const BufferCRef& key = split->split[is].key;

			char* kt = NULL;
			if(! key.isNull())
			{
				kt = (char*)::alloca(key.size());
				::memcpy(kt, key.get(), key.size());
			}
			kps.push_back(make_pair(BufferCRef(kt, key.size()), split->split[is].pgid));
		}
		for(; i < numKeys; ++ i)
		{
			ALLOC_COPY_OLDKP(i);
			kps.push_back(e);
		}

#undef ALLOC_COPY_OLDKP
	}

	split->reset();

	int i = 0, iE = kps.size();

	Node ret = Node(pio->readPage(pageOrigId())); // Node pg carrying split->pgidFollow

	// set up old node
	ovr.initBody(ptrm1_);
	for(; i < iE; ++ i)
	{
		if(ovr.footer().sizeFree + packedsize(kps[i].first) + sizeof(page_id_t) + sizeof(uint16_t)*2 > thresSplit) break;

		ovr.kp_offset(i) = ovr.addKP(kps[i]);
	}
	pio->sync(ovr);
	if(i == iE)
	{
		// all kps stored into old node...

		// split was actually not needed!
		return ret;
	}

	Node newNode = pio->newInitPage<Node>();

	// setup splitinfo to be passed upstream
	{
		const kp_t& kp = kps[i++];

		split->pgidSplit = pageOrigId();
		split->addSplit(kp.first, newNode.pageId());

		newNode.initBody(kp.second);
	}

	if(iFollow >= i)
	{
		ret = newNode; // target child went into split node
	}

	// set up new node
	// newNode.initBody(); // already done
	int off = i;
	for(; i < iE; ++ i)
	{
		newNode.kp_offset(i - off) = newNode.addKP(kps[i]);
	}
	pio->sync(newNode);

	split->pgidFollow = ret.pageOrigId();
	return ret;
}

Node
Node::handleChildDel(page_id_t* pgidNextChild, page_id_t pgid, bool* bOvr, PageIO* pio)
{
	const int numKeys = footer().numKeys;
	page_id_t ptrm1_ = ptrm1();
	if(numKeys == 0)
	{
		PTNK_ASSERT(pgid == ptrm1_);

		if(pgidNextChild) *pgidNextChild = PGID_INVALID;
		pio->discardPage(pageOrigId());
		return Node(Page(), true);
	}

	Node ovr(pio->modifyPage(*this, bOvr));

	// find the old child page & copy kps
	std::vector<kp_t> kps; kps.reserve(512);

	if(pageId() != ovr.pageId())
	{
		// if operation result is to be saved on different page,
		// the KPs stored on the old page is valid throughout the op

		if(pgid == ptrm1_)
		{
			// ptrm1 is the child to be removed
			{
				ptrm1_ = ptr(0);	
				if(pgidNextChild) *pgidNextChild = ptrm1_;
			}
			for(int i = 1; i < numKeys; ++ i)
			{
				kps.push_back(kp(i));
			}
		}
		else
		{
			// ptr(i >= 0) is the child to be removed

			for(int i = 0; i < numKeys; ++ i)
			{
				kp_t e = kp(i);

				if(e.second == pgid)
				{
					if(pgidNextChild)
					{
						*pgidNextChild = (i != numKeys-1) ? kp(i+1).second : PGID_INVALID;
					}

					continue; // skip the child to be removed
				}

				kps.push_back(e);
			}
		}
	}
	else
	{
		// if operation is done in-place, we need to keep copy of old kps
		// the copy is allocated from the stack by alloca
#define ALLOC_COPY_OLDKP(i) \
		kp_t e = kp(i); \
		char *kt = NULL; \
		if(! e.first.isNull()) { kt = (char*)::alloca(e.first.size()); ::memcpy(kt, e.first.get(), e.first.size()); e.first = BufferCRef(kt, e.first.size()); }

		if(pgid == ptrm1_)
		{
			// ptrm1 is the child to be removed
			{
				ptrm1_ = ptr(0);	
				if(pgidNextChild) *pgidNextChild = ptrm1_;
			}
			for(int i = 1; i < numKeys; ++ i)
			{
				ALLOC_COPY_OLDKP(i);
				kps.push_back(e);
			}
		}
		else
		{
			// ptr(i >= 0) is the child to be removed

			for(int i = 0; i < numKeys; ++ i)
			{
				ALLOC_COPY_OLDKP(i);

				if(e.second == pgid)
				{
					if(pgidNextChild)
					{
						*pgidNextChild = (i != numKeys-1) ? kp(i+1).second : PGID_INVALID;
					}

					continue; // skip the child to be removed
				}

				kps.push_back(e);
			}
		}

#undef ALLOC_COPY_OLDKP
	}

	// put kps back to the node
	ovr.initBody(ptrm1_);
	int i, iM = kps.size();
	for(i = 0; i < iM; ++ i)
	{
		ovr.kp_offset(i) = ovr.addKP(kps[i]);
	}
	pio->sync(ovr);

	return ovr;
}

void
Node::updateLinks_(mod_info_t* mod, PageIO* pio)
{
	// clone old node (FIXME: do this only if ovr == this)
	char temp_buf[PTNK_PAGE_SIZE];
	::memcpy(temp_buf, getRaw(), PTNK_PAGE_SIZE);
	Node temp(Page(temp_buf, false));

	bool upd = false;
	unsigned int numKeys = footer().numKeys;
	{
		page_id_t idOld = temp.ptrm1();
		page_id_t idNew = pio->updateLink(idOld);

		if(idNew != idOld) upd = true;
		temp.ptrm1() = idNew;
	}
	for(unsigned int i = 0; i < numKeys; ++ i)
	{
		page_id_t idOld = temp.ptr(i);
		page_id_t idNew = pio->updateLink(idOld);

		if(idNew != idOld) upd = true;
		temp.ptr(i) = idNew;
	}

	if(upd)
	{
		Node ovr(pio->modifyPage(*this, mod));
		::memcpy(ovr.rawbody(), temp.rawbody(), BODY_SIZE);

		pio->sync(ovr);
	}
}

page_id_t
Node::ptrBefore(page_id_t p) const
{
	if(p == ptrm1())
	{
		// no ptr before
		return PGID_INVALID;
	}

	int i, numKeys = footer().numKeys;
	for(i = 0; i < numKeys; ++ i)
	{
		if(ptr(i) == p) break;
	}
	if(i == numKeys) PTNK_THROW_RUNTIME_ERR("specified ptr not found");

	if(i != 0)
	{
		return ptr(i-1);
	}
	else
	{
		return ptrm1();
	}
}

page_id_t
Node::ptrAfter(page_id_t p) const
{
	int i, numKeys = footer().numKeys;
	if(ptrm1() == p)
	{
		i = -1;
	}
	else
	{
		for(i = 0; i < numKeys; ++ i)
		{
			if(ptr(i) == p) break;
		}
	}
	if(i == numKeys)
	{
		PTNK_THROW_RUNTIME_ERR("specified ptr not found");
	}
	if(i == numKeys - 1)
	{
		// no ptr after
		return PGID_INVALID;
	}

	++ i;

	if(i >= 0)
	{
		return ptr(i);
	}
	else
	{
		return ptrm1();
	}
}

void
Node::dump_(PageIO* pio) const
{
	dumpHeader();
	std::cout << "- Node <numKeys: " << footer().numKeys << ", sizeFree: " << footer().sizeFree << ">" << std::endl;

	std::string out1("  "), out2("  ");
	out1 +=      " |*|";
	out2 += pgid2str(ptrm1());
	unsigned int i, numKeys = footer().numKeys;
	for(i = 0; i < numKeys; ++ i)
	{
		BufferCRef k; page_id_t p;
		boost::tie(k, p) = kp(i);

		out1 += k.inspect(); out1 += "  |*|";
		out2 += "    "; out2 += pgid2str(p);
	}
	puts(out1.c_str()); puts(out2.c_str());
	puts("");

	if(pio)
	{
		// Dump child pages	
		if(ptrm1() == pageId())
		{
			printf("body()->ptr[-1] == this.pageid\n");
		}
		else
		{
			pio->readPage(ptrm1()).dump(pio);
		}

		unsigned int i, numKeys = footer().numKeys;
		for(i = 0; i < numKeys; ++ i)
		{
			BufferCRef k; page_id_t p;
			boost::tie(k, p) = kp(i);

			if(p == pageId())
			{
				printf("body()->ptr[%d] == this.pageid\n", i);
				continue;
			}
			
			pio->readPage(p).dump(pio);
		}
	}
}

void
Node::dumpGraph_(FILE* fp, PageIO* pio) const
{
	fprintf(fp, "\"page%u\" [\n", (unsigned int)pageId());
	fprintf(fp, "label = <<TABLE><TR>");
	if(pageOvrTgt() != PGID_INVALID)
	{
		fprintf(fp, "<TD PORT=\"head\">Node [id: %u ovr: %u]</TD>", (unsigned int)pageId(), (unsigned int)pageOvrTgt());
	}
	else
	{
		fprintf(fp, "<TD PORT=\"head\">Node [id: %u orig!]</TD>", (unsigned int)pageId());
	}
	unsigned int i, numKeys = footer().numKeys;
	fprintf(fp, "<TD PORT=\"pm1\" BGCOLOR=\"yellow\"><FONT COLOR=\"blue\">%u</FONT></TD>", (unsigned int)ptrm1());
	for(i = 0; i < numKeys; ++ i)
	{
		BufferCRef k; page_id_t p;
		boost::tie(k, p) = kp(i);
		
		if(i < numKeys)
		{
			fprintf(fp, "<TD>%u</TD>", *(int*)k.get());
		}
		fprintf(fp, "<TD PORT=\"p%u\" BGCOLOR=\"yellow\"><FONT COLOR=\"blue\">%u</FONT></TD>", i, (unsigned int)p);
	}
	fprintf(fp, "</TR></TABLE>>\nshape=plaintext\n");
	fprintf(fp, "];\n");

	if(pio)
	{
		// Dump child pages	
		unsigned int i, numKeys = footer().numKeys;
		{
			page_id_t p = ptrm1();
			if(p == pageId())
			{
				printf("body()->ptr[-1] == this.pageid\n");
			}
			else
			{
				Page pg(pio->readPage(p));
				pg.dumpGraph(fp, pio);

				// draw conn
				fprintf(fp, "\"page%u\":pm1 -> \"page%u\":head;\n", (unsigned int)pageId(), (unsigned int)pg.pageId());
			}
		}
		for(i = 0; i < numKeys; ++ i)
		{
			page_id_t p = ptr(i);
		
			if(p == pageId())
			{
				printf("body()->ptr[%d] == this.pageid\n", i);
				continue;
			}

			Page pg(pio->readPage(p));
			pg.dumpGraph(fp, pio);

			// draw conn
			fprintf(fp, "\"page%u\":p%u -> \"page%u\":head;\n", (unsigned int)pageId(), i, (unsigned int)pg.pageId());
		}
	}
}

bool
Node::refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	if(*cursor && numPages == 0)
	{
		// free cursor
		Buffer* bufKeyResume = static_cast<Buffer*>(*cursor);
		delete bufKeyResume;

		*cursor = NULL;
	}

	btree_cursor_t bc;

	Buffer* bufKeyResume;
	if(*cursor)
	{
		bufKeyResume = static_cast<Buffer*>(*cursor);
		query_t q;
		q.type = MATCH_OR_NEXT;
		q.key = bufKeyResume->rref();

		btree_query(&bc, pageOrigId(), q, pio);
	}
	else
	{
		// new cursor
		*cursor = bufKeyResume = new Buffer;
		btree_cursor_front(&bc, pageOrigId(), pio);
	}

	bool bProp = false;
	while(numPages > 0 && bc.leaf.isValid())
	{
		if(bc.leaf.pageId() < threshold) // not pageOrigId
		{
			mod_info_t mod;
			Leaf leafNew(pio->modifyPage(bc.leaf, &mod));
			pio->sync(leafNew);
			PTNK_ASSERT(mod.isValid());
			
			// propagate page w/ old link
			// FIXME: below code dup from btree_put -> maybe include in btree_cursor_t?
			VNode::const_reverse_iterator itNodes = bc.nodes.rbegin(), itE = bc.nodes.rend();
			while(itNodes != itE)
			{
				Page pgNode(*itNodes); ++ itNodes;

				pio->notifyPageWOldLink(pgNode.pageOrigId());
			}
			bProp = true;

			-- numPages; 
		}
		
		btree_cursor_nextleaf(&bc, pio);
	}

	if(bc.leaf.isValid())
	{
		*bufKeyResume = Leaf(bc.leaf).keyFirst();
	}
	else
	{
		// free cursor
		delete bufKeyResume;
		*cursor = NULL;
	}

	return bProp;
}

namespace 
{

void leaf_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ /* NOP */ }

void leaf_dump(const Page& pg, PageIO* pio)
{ Leaf(pg).dump_(); }

void leaf_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ Leaf(pg).dumpGraph_(fp); }

static Page::dyndispatcher_t g_leaf_handlers = 
{
	leaf_updateLinks,
	leaf_dump,
	leaf_dumpGraph
};

Page::register_dyndispatcher g_leaf_reg(PT_LEAF, &g_leaf_handlers);

} // end of anonymous namespace

void
Leaf::init(page_id_t id)
{
	initHdr(id, PT_LEAF);
	initBody();
}

inline
void
Leaf::initBody()
{
	footer().numKVs = 0;
	footer().sizeFree = BODY_SIZE - sizeof(footer_t);
}

// #define VERBOSE_IDX

pair<int, bool>
Leaf::idx_lower_bound(int b, int e, BufferCRef key) const
{
#ifdef VERBOSE_IDX
	std::cout << "idx_lower_bound key: " << key << std::endl;
#endif
	int d = e - b, d2, ms, m, diff;
	bool isExact = false;
	while(d > 0)
	{
#ifdef VERBOSE_IDX
		std::cout << "b: " << b << " d: " << d << std::endl;
#endif
		d2 = d >> 1;

		// suppose key = 3

		// idx: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 
		// key: 1 1 1 2 2 3 3 3 3 3  4  4  4  4  5  5
		// 
		// initially, ms = m = 0 + 16/2 = 8

		ms = m = b + d2;
		
		uint16_t kvo;
		while((kvo = kv_offset(ms)) & VALUE_ONLY)
		{
			-- ms;
		}

#ifdef VERBOSE_IDX
		std::cout << "m: " << m << " ms: " << ms << std::endl;
#endif

		// ms = 5, m = 8
		
		const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
		BufferCRef keyref(false /* no init as invalid */);
		if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
		{
			keyref = BufferCRef::NULL_VAL;	
		}
		else
		{
			keyref = BufferCRef(kv->offset, kv->szKey);
		}
		
		diff = bufcmp(keyref, key);
#ifdef VERBOSE_IDX
		std::cout << "bufcmp " << keyref << " and " << key << " => " << diff << std::endl;
#endif
		if(diff == 0) isExact = true;

		if(diff < 0)
		{
			int ee = b + d;
			b = m;
			if(kv_offset(b) & VALUE_ONLY)
			{
				do
				{
					++ b;
				}
				while(kv_offset(b) & VALUE_ONLY);
			}
			else
			{
				++ b;
			}

			d = ee - b;
		}
		else
		{
			d = ms - b;
		}
	}
#ifdef VERBOSE_IDX
	std::cout << "result " << b << std::endl;
#endif
	return make_pair(b, isExact);
}

pair<int, bool>
Leaf::idx_upper_bound(int b, int e, BufferCRef key) const
{
#ifdef VERBOSE_IDX
	std::cout << "idx_upper_bound key: " << key << std::endl;
#endif
	int d = e - b, d2, ms, m, diff;
	bool foundExact = false;
	while(d > 0)
	{
#ifdef VERBOSE_IDX
		std::cout << "b: " << b << " d: " << d << std::endl;
#endif
		d2 = d >> 1;

		// suppose key = 3

		// idx: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 
		// key: 1 1 1 2 2 3 3 3 3 3  4  4  4  4  5  5
		// 
		// initially, ms = m = 0 + 16/2 = 8

		ms = m = b + d2;
		
		uint16_t kvo;
		while((kvo = kv_offset(ms)) & VALUE_ONLY)
		{
			-- ms;
		}

#ifdef VERBOSE_IDX
		std::cout << "m: " << m << " ms: " << ms << std::endl;
#endif

		// ms = 5, m = 8
		
		const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
		BufferCRef keyref(false /* no init as invalid */);
		if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
		{
			keyref = BufferCRef::NULL_VAL;	
		}
		else
		{
			keyref = BufferCRef(kv->offset, kv->szKey);
		}
		
		diff = bufcmp(keyref, key);
#ifdef VERBOSE_IDX
		std::cout << "bufcmp " << keyref << " and " << key << " => " << diff << std::endl;
#endif
		if(diff == 0) foundExact = true;

		if(diff <= 0)
		{
			int ee = b + d;
			b = m;
			if(kv_offset(b) & VALUE_ONLY)
			{
				do
				{
					++ b;
				}
				while(kv_offset(b) & VALUE_ONLY);
			}
			else
			{
				++ b;
			}

			d = ee - b;
		}
		else
		{
			d = ms - b;
		}
	}
#ifdef VERBOSE_IDX
	std::cout << "result " << b << std::endl;
#endif
	return make_pair(b, foundExact);
}

BufferCRef
Leaf::keyFirst() const
{
	uint16_t kvo = kv_offset(0);
	PTNK_ASSERT(! (kvo & VALUE_ONLY));

	const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
	if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
	{
		return BufferCRef::NULL_VAL;
	}
	else
	{
		return BufferCRef(kv->offset, kv->szKey);	
	}
}

void
Leaf::query(btree_cursor_t* cursor, const query_t& q) const
{
	PTNK_ASSERT(cursor->leaf.isValid());
	PTNK_ASSERT(cursor->leaf.pageId() == pageId());
	PTNK_ASSERT(q.isValid());

	int numKVs = footer().numKVs;

	if(q.type & F_NOSEARCH)
	{
		switch(q.type)
		{
		case FRONT:
			if(numKVs > 0)
			{
				cursor->idx = 0;
			}
			else
			{
				cursor->idx = btree_cursor_t::NO_MATCH;
			}
			return;

		case BACK:
			if(numKVs > 0)
			{
				cursor->idx = numKVs - 1;
			}
			else
			{
				cursor->idx = btree_cursor_t::NO_MATCH;
			}
			return;

		default:
			PTNK_THROW_RUNTIME_ERR("unknown query type (w/ F_NOSEARCH)");
		}
	}

	int i;
	if(q.type & F_LOWER_BOUND)
	{
		bool isExact;
		boost::tie(i, isExact) = idx_lower_bound(0, numKVs, q.key);

		if(q.type == MATCH_EXACT)
		{
			if(i == numKVs || ! isExact)
			{
				// no match!
				cursor->idx = btree_cursor_t::NO_MATCH;
				return;
			}
		}
		else if(q.type == MATCH_OR_PREV)
		{
			if(i == numKVs || ! isExact)
			{
				-- i;
			}
		}
		else if(q.type == BEFORE)
		{
			-- i;
		}
	}
	else /* F_UPPER_BOUND */
	{
		bool foundExact;
		boost::tie(i, foundExact) = idx_upper_bound(0, numKVs, q.key);

		if(q.type == AFTER)
		{
			/* NOP */
		}
		else
		{
			if(i > 0 && foundExact)
			{
				// select last matching kv
				-- i; 
			}
		}
	}

	cursor->idx = i;
}

void
Leaf::cursorGet(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, const btree_cursor_t& cursor) const
{
	PTNK_ASSERT(cursor.leaf.pageId() == pageId());
	int i = cursor.idx;
	if(i < 0 || footer().numKVs <= i)
	{
		// out of idx
		if(szKey) *szKey = -1;
		if(szValue) *szValue = -1;

		return;	
	}
	
	uint16_t kvo = kv_offset(i);
	if(! (kvo & VALUE_ONLY))
	{
		const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
		size_t packedszKey;
		if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
		{
			packedszKey = 0;
			if(szKey) { *szKey = Buffer::NULL_TAG; }
		}
		else
		{
			packedszKey = kv->szKey;
			if(szKey) { *szKey = bufcpy(key, BufferCRef(kv->offset, packedszKey)); }
		}

		if(szValue)
		{
			if(PTNK_UNLIKELY(kv->szValue == NULL_TAG))
			{
				*szValue = Buffer::NULL_TAG;
			}
			else
			{
				*szValue = bufcpy(value, BufferCRef(kv->offset + packedszKey, kv->szValue));
			}
		}
	}
	else
	{
		if(szValue)
		{
			const packedv_t* v = reinterpret_cast<const packedv_t*>(rawbody() + kvo - VALUE_ONLY);
			
			if(PTNK_UNLIKELY(v->szValue == NULL_TAG))
			{
				*szValue = Buffer::NULL_TAG;
			}
			else
			{
				*szValue = bufcpy(value, BufferCRef(v->offset, v->szValue));
			}
		}

		if(szKey)
		{
			do
			{
				-- i;	
			}
			while((kvo = kv_offset(i)) & VALUE_ONLY);

			const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
			if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
			{
				*szKey = Buffer::NULL_TAG;
			}
			else
			{
				*szKey = bufcpy(key, BufferCRef(kv->offset, kv->szKey));
			}
		}
	}
}

ssize_t
Leaf::cursorGetValue(BufferRef value, const btree_cursor_t& cursor) const
{
	ssize_t ret;
	cursorGet(BufferRef(), NULL, value, &ret, cursor);
	return ret;
}

void
Leaf::cursorPut(btree_cursor_t* cur, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio)
{
	PTNK_ASSERT(cur->leaf.pageId() == pageId());

	int i = cur->idx;
	if(i < 0 || footer().numKVs <= i)
	{
		PTNK_THROW_RUNTIME_ERR("Leaf::cursorPut: out of idx");
	}

	Leaf ovr(pio->modifyPage(*this, bNotifyOldLink));
	updateIdx(ovr, i, value, split, pio);
	cur->leaf = pio->readPage(pageOrigId()); // re-read leaf

	// fix cursor
	if(split->isValid())
	{
		if(i < ovr.numKVs())
		{
			// cur->idx preserved
			split->pgidFollow = pageOrigId();
		}
		else
		{
			i -= ovr.numKVs();
			for(unsigned int iS = 0; iS < split->numSplit; ++ iS)
			{
				Leaf lS(pio->readPage(split->split[iS].pgid));
				if(i < lS.numKVs())
				{
					cur->leaf = lS;
					cur->idx = i;
					split->pgidFollow = lS.pageOrigId();
					break;
				}
			}
			// *** not reached ***
			PTNK_ASSERT(false);
		}
	}
}

ssize_t
Leaf::get(BufferCRef key, BufferRef value) const
{
	PTNK_ASSERT(key.isValid());

	int i; bool isExact;
	boost::tie(i, isExact) = idx_lower_bound(0, footer().numKVs, key);
	if(i == footer().numKVs) return -1;

	if(isExact)
	{
		return bufcpy(value, getV(i));
	}
	else
	{
		return -1;
	}
}

inline
bool
Leaf::isRoomForKVAvailable(BufferCRef key, BufferCRef value) const
{
	PTNK_ASSERT(key.isValid());
	PTNK_ASSERT(value.isValid());

	if(footer().numKVs == 255) return false;

	int sizeFree = footer().sizeFree;
	sizeFree -= packedsize(key) + packedsize(value) + sizeof(uint16_t)*3;
	return sizeFree >= 0;
}

inline
bool
Leaf::isRoomForVAvailable(BufferCRef value) const
{
	PTNK_ASSERT(value.isValid());

	if(footer().numKVs == 255) return false;

	int sizeFree = footer().sizeFree;
	sizeFree -= packedsize(value) + sizeof(uint16_t)*3;
	return sizeFree >= 0;
}

void
Leaf::insert(BufferCRef key, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio, bool bAbortOnExisting)
{
	PTNK_ASSERT(key.isValid());
	PTNK_ASSERT(value.isValid());
	PTNK_ASSERT(key.size() < BODY_SIZE/2);
	PTNK_ASSERT(value.size() < BODY_SIZE/2);

	Leaf ovr(pio->modifyPage(*this, bNotifyOldLink));

	// find the new kv idx
	int new_i; bool foundExact;
	boost::tie(new_i, foundExact) = idx_upper_bound(0, numKVs(), key);

	if(! foundExact)
	{
		insertIdxNoExact(ovr, new_i, key, value, split, pio);
	}
	else
	{
		if(bAbortOnExisting)
		{
			throw ptnk_duplicate_key_error();
		}

		// add new value-only record to this leaf

		if(PTNK_LIKELY(isRoomForVAvailable(value)))
		{
			// new value-only record fit in this leaf

			uint16_t newv_offset = ovr.addV(value) + VALUE_ONLY;

			// shift kvs after
			for(int j = ovr.numKVs()-1; j > new_i; -- j)
			{
				ovr.kv_offset(j) = kv_offset(j-1);
			}

			ovr.kv_offset(new_i) = newv_offset;

			split->reset();
		}
		else
		{
			// no room for new record
			
			// need to split
		#ifdef VERBOSE_LEAF_SPLIT
			std::cout << "insertSplit happened at k " << key.inspect() << " v " << value.inspect() << std::endl;
		#endif

			char tmpbuf[BODY_SIZE];
			VKV kvs; kvs.reserve(numKVs() + 1);
			#ifdef FIXME_REF
			if(bNotifyOldLink)
			{
				kvsRefInsert(kvs, BufferCRef::INVALID_VAL, value, new_i);
			}
			else
			#endif
			{
				kvsCopyInsert(kvs, BufferCRef::INVALID_VAL, value, new_i, tmpbuf);
			}

			// if bulk insert, split at last
			size_t thresSplit = (numKVs() == new_i) ? 0 : BODY_SIZE/2;
			doSplit(kvs, ovr, thresSplit, split, pio);
		}
	}

	pio->sync(ovr);
}

void
Leaf::insertIdxNoExact(Leaf ovr, int new_i, BufferCRef key, BufferCRef value, btree_split_t* split, PageIO* pio)
{
	if(PTNK_LIKELY(isRoomForKVAvailable(key, value)))
	{
		// new kv fit in this leaf...
		// no need to split

		uint16_t newkv_offset = ovr.addKV(key, value);
		
		// shift kvs after
		for(int j = ovr.numKVs()-1; j > new_i; -- j)
		{
			ovr.kv_offset(j) = ovr.kv_offset(j-1);
		}

		ovr.kv_offset(new_i) = newkv_offset;

		split->reset();
	}
	else
	{
		// no room for new kv
		
		// need to split
	#ifdef VERBOSE_LEAF_SPLIT
		std::cout << "insertSplit happened at k " << key.inspect() << " v " << value.inspect() << std::endl;
	#endif

		char tmpbuf[BODY_SIZE];
		VKV kvs; kvs.reserve(numKVs() + 1);
		#ifdef FIXME_REF
		if(mod->isValid())
		{
			kvsRefInsert(kvs, key, value, new_i);
		}
		else
		#endif
		{
			kvsCopyInsert(kvs, key, value, new_i, tmpbuf);
		}

		// if bulk insert, split at last
		size_t thresSplit = (numKVs() == new_i) ? 0 : BODY_SIZE/2;
		doSplit(kvs, ovr, thresSplit, split, pio);
	}
}

void
Leaf::updateIdx(Leaf ovr, int i, BufferCRef value, btree_split_t* split, PageIO* pio)
{
	PTNK_ASSERT(0 <= i && i < footer().numKVs);
	ssize_t neededspace;
	{
		uint16_t kvo = kv_offset(i);

		// strategy 1: in-place value update!
		if(kvo & VALUE_ONLY)
		{
			// in-place update value-only record

			packedv_t* v = reinterpret_cast<packedv_t*>(ovr.rawbody() + kvo - VALUE_ONLY);
			ssize_t packed_szValue = v->szValue == NULL_TAG ? 0 : v->szValue;

			// FIXME: packedsize() contains call to isNull and isNull check is also checked below
			neededspace = value.packedsize() - packed_szValue;
			if(neededspace <= 0)
			{
				// new value requires same or less space than the old value...
			
				if(PTNK_UNLIKELY(value.isNull()))
				{
					v->szValue = NULL_TAG;
				}
				else
				{
					::memcpy(v->offset, value.get(), value.size());
					v->szValue = value.size();
				}

				// if new value size < old value size, fragmentation will occur
				// however, we are going to leave it for now.
				
				pio->sync(ovr);
				return;
			}
		}
		else
		{
			// in-place update regular key-value record

			packedkv_t* kv = reinterpret_cast<packedkv_t*>(ovr.rawbody() + kvo);
			ssize_t packed_szValue = kv->szValue == NULL_TAG ? 0 : kv->szValue;

			// FIXME: packedsize() contains call to isNull and isNull check is also checked below
			neededspace = value.packedsize() - packed_szValue;
			if(neededspace <= 0)
			{
				// new value requires same or less space than the old value...

				if(PTNK_UNLIKELY(value.isNull()))
				{
					kv->szValue = NULL_TAG;
				}
				else
				{
					char* offValue = kv->offset;
					if(PTNK_LIKELY(kv->szKey != NULL_TAG)) offValue += kv->szKey;

					::memcpy(offValue, value.get(), value.size());
					kv->szValue = value.size();
				}

				// if new value size < old value size, fragmentation will occur
				// however, we are going to leave it for now.
				
				pio->sync(ovr);
				return;
			}
		}
	}

	// prepare kvs for strategy 2 or 3
	char tmpbuf[BODY_SIZE];
	VKV kvs;
	#ifdef FIXME_REF
	if(mod->isValid())
	{
		kvsRef(kvs);
	}
	else
	#endif
	{
		kvsCopyAll(kvs, tmpbuf);
	}
	kvs[i].second = value; // update value at idx i

	if(neededspace <= footer().sizeFree)
	{
		// strategy 2: defrag leaf and update value
		
		doDefrag(kvs, ovr, pio);
	}
	else
	{
		// strategy 3: perform split and update value
		
		doSplit(kvs, ovr, BODY_SIZE/2, split, pio);
	}
}

void
Leaf::update(BufferCRef key, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio)
{
	PTNK_ASSERT(key.isValid());
	PTNK_ASSERT(value.isValid());
	
	Leaf ovr(pio->modifyPage(*this, bNotifyOldLink));
	
	// find the matching kv pair
	int i; bool isExact;
	boost::tie(i, isExact) = idx_lower_bound(0, numKVs(), key);
	if(i == numKVs() || ! isExact)
	{
		// no existing kv pair! perform insert instead.
		//
		// NOTE: This fallback is done here as idx_lower_bound performs better when
		//       the whole page content is cached due to memcpy occuring at pio->modifyPage.
		
		insertIdxNoExact(ovr, i, key, value, split, pio);
		pio->sync(ovr);
		return;
	}

	updateIdx(ovr, i, value, split, pio);
	pio->sync(ovr);
}

bool
Leaf::cursorDelete(btree_cursor_t* cur, bool* bOvr, PageIO* pio)
{
	int i = cur->idx;

	PTNK_ASSERT(0 <= i || i < footer().numKVs);
	if(footer().numKVs <= 1)
	{
		return false;
	}

	Leaf ovr(pio->modifyPage(*this, bOvr));

	char tmpbuf[BODY_SIZE];
	VKV kvs;
	#ifdef FIXME_REF
	if(mod->isValid())
	{
		kvsRefButIdx(kvs, i);
	}
	else
	#endif
	{
		kvsCopyButIdx(kvs, i, tmpbuf);
	}

	doDefrag(kvs, ovr, pio);

	cur->leaf = pio->readPage(pageOrigId()); // re-read leaf
	return true;
}

inline
void
Leaf::kvsRef(VKV& kvs) const
{
	PTNK_THROW_RUNTIME_ERR("not impl");
}

inline
void
Leaf::kvsRefButIdx(VKV& kvs, int idx) const
{
	PTNK_THROW_RUNTIME_ERR("not impl");
}

inline
void
Leaf::kvsRefInsert(VKV& kvs, BufferCRef key, BufferCRef value, int new_i) const
{
	PTNK_THROW_RUNTIME_ERR("not impl");
}

void
Leaf::kvsCopy(VKV& kvs, int b, int e, BufferCRef* keyLast, char** tmpbuf) const
{
	// if operation is done in-place, we need to keep copy of old kvs
	// the copy is allocated from _tmpbuf_
	
	int i; BufferCRef ki;
	for(i = b; i < e; ++ i)
	{
		uint16_t kvo = kv_offset(i);
		if(kvo & VALUE_ONLY)
		{
			const packedv_t* v = reinterpret_cast<const packedv_t*>(rawbody() + kvo - VALUE_ONLY);
			BufferCRef vi(false);
			if(PTNK_UNLIKELY(v->szValue == NULL_TAG))
			{
				vi = BufferCRef::NULL_VAL;
			}
			else
			{
				::memcpy(*tmpbuf, v->offset, v->szValue);
				vi = BufferCRef(*tmpbuf, v->szValue);
				*tmpbuf += v->szValue;
			}

			kvs.push_back(make_pair(BufferCRef::INVALID_VAL, vi));
		}
		else
		{
			const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);
			size_t packedszKey;
			if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
			{
				packedszKey = 0;
				ki = BufferCRef::NULL_VAL;
			}
			else
			{
				packedszKey = kv->szKey;

				::memcpy(*tmpbuf, kv->offset, kv->szKey);
				ki = BufferCRef(*tmpbuf, kv->szKey);
				*tmpbuf += kv->szKey;
			}

			if(i == b && bufeq(*keyLast, ki))
			{
				// [key] denotes kv record with key _key_
				//
				//      /- b
				//      v
				// [a] [a] [<-] [<-] [<-]
				//  ^
				//  \- new inserted record 
				//
				// in this case, record[b].key must be [<-]

				ki = BufferCRef::INVALID_VAL;	
			}

			if(PTNK_UNLIKELY(kv->szValue == NULL_TAG))
			{
				kvs.push_back(make_pair(ki, BufferCRef::NULL_VAL));
			}
			else
			{
				::memcpy(*tmpbuf, kv->offset + packedszKey, kv->szValue);
				kvs.push_back(make_pair(ki, BufferCRef(*tmpbuf, kv->szValue)));
				*tmpbuf += kv->szValue;
			}
		}
	}

	*keyLast = ki;
}

inline
void
Leaf::kvsCopyAll(VKV& kvs, char* tmpbuf) const
{
	BufferCRef keyLast = BufferCRef::INVALID_VAL;
	kvsCopy(kvs, 0, footer().numKVs, &keyLast, &tmpbuf);
}

void
Leaf::kvsCopyButIdx(VKV& kvs, int idx, char* tmpbuf) const
{
	BufferCRef keyLast = BufferCRef::INVALID_VAL;
	kvsCopy(kvs, 0, idx, &keyLast, &tmpbuf);

	// the record to be deleted was the last record in the leaf
	if(idx+1 >= footer().numKVs) return;

	// read key of the record[idx] to be erased
	uint16_t kvo = kv_offset(idx);
	uint16_t kvo_next = kv_offset(idx+1);
	if(kvo & VALUE_ONLY				// removal of value only record
	|| !(kvo_next & VALUE_ONLY))	// next record is not value only record  
	{
		kvsCopy(kvs, idx+1, footer().numKVs, &keyLast, &tmpbuf);
	}
	else
	{
		// if record[idx+1] is a VALUE_ONLY record, special care needs to be done.
		//
		//  /- the record to be deleted
		//  v
		// [a] [<-] [<-]
		//
		// becomes
		//
		//     [a]  [<-]
		//      ^
		//      \- key added to previous value only record

		BufferCRef keyAtIdx(false);
		{
			const packedkv_t* kv = reinterpret_cast<const packedkv_t*>(rawbody() + kvo);

			if(PTNK_UNLIKELY(kv->szKey == NULL_TAG))
			{
				keyAtIdx = BufferCRef::NULL_VAL;
			}
			else
			{
				::memcpy(tmpbuf, kv->offset, kv->szKey);
				keyAtIdx = BufferCRef(tmpbuf, kv->szKey);
				tmpbuf += kv->szKey;
			}
		}

		BufferCRef valNext(false);
		{
			const packedv_t* v = reinterpret_cast<const packedv_t*>(rawbody() + kvo_next - VALUE_ONLY);
			
			if(PTNK_UNLIKELY(v->szValue == NULL_TAG))
			{
				valNext = BufferCRef::NULL_VAL;
			}
			else
			{
				::memcpy(tmpbuf, v->offset, v->szValue);
				valNext = BufferCRef(tmpbuf, v->szValue);
				tmpbuf += v->szValue;
			}
		}

		// record[idx+1] is now made regular key-value record
		kvs.push_back(make_pair(keyAtIdx, valNext));

		// handle records rest
		kvsCopy(kvs, idx+2, footer().numKVs, &keyLast, &tmpbuf);
	}
}

void
Leaf::kvsCopyInsert(VKV& kvs, BufferCRef key, BufferCRef value, int new_i, char* tmpbuf) const
{
	BufferCRef keyLast = BufferCRef::INVALID_VAL;
	kvsCopy(kvs, 0, new_i, &keyLast, &tmpbuf);
	kvs.push_back(make_pair(key, value));
	kvsCopy(kvs, new_i, footer().numKVs, &keyLast, &tmpbuf);
}

void
Leaf::doDefrag(const VKV& kvs, Leaf ovr, PageIO* pio)
{
	ovr.footer().numKVs = 0;
	ovr.footer().sizeFree = BODY_SIZE - sizeof(footer_t);

	const int iE = kvs.size();
	for(int i = 0; i < iE; ++ i)
	{
		const KV& kv = kvs[i];
		if(kv.first.isValid())
		{
			ovr.kv_offset(i) = ovr.addKV(kv);
		}
		else
		{
			ovr.kv_offset(i) = ovr.addV(kv.second) + VALUE_ONLY;	
		}
	}

	pio->sync(ovr);
}

// #define VERBOSE_SPLIT

void
Leaf::doSplit(const VKV& kvs, Leaf ovr, size_t thresSplit, btree_split_t* split, PageIO* pio)
{
	PTNK_ASSERT(thresSplit <= BODY_SIZE/2);

	split->reset();

	Leaf active;
	bool oldUsed = false; // flag which is set true when ovr leaf has been used

	int iLeaf = 0;
	int iKeyS = 0;
	const int iE = static_cast<int>(kvs.size());
	size_t sizeFree = 0;
	while(iKeyS < iE)
	{
		size_t packedsize = 0;
		BufferCRef key = kvs[iKeyS].first;
		{
			const KV& r = kvs[iKeyS];

			// regular key-value record
			packedsize += r.first.packedsize() + r.second.packedsize() + sizeof(uint16_t)*3;
		}
		int iKeyE = iKeyS+1;
		while(iKeyE < iE)
		{
			const KV& r = kvs[iKeyE];
			if(r.first.isValid()) break;

			// value-only record
			packedsize += r.second.packedsize() + sizeof(uint16_t)*2;

			++ iKeyE;
		}
		int nKeys = iKeyE - iKeyS;

	#ifdef VERBOSE_SPLIT
		std::cout << "key: " << key << std::endl;
		std::cout << "iks: " << iKeyS << " e: " << iKeyE << std::endl;
		std::cout << "packedsize: " << packedsize << " sizeFree: " << sizeFree << std::endl;
	#endif

		if(sizeFree < thresSplit || sizeFree < packedsize || (int)active.footer().numKVs + nKeys > MAX_NUM_KVS)
		{
			if(packedsize < BODY_SIZE*2/3)
			{
			#ifdef VERBOSE_SPLIT
				std::cout << "new normal leaf" << std::endl;
			#endif

				// switch to leaf (normal leaf)
				if(! oldUsed)
				{
					active = ovr;
				}
				else
				{
					active = Leaf(pio->newInitPage<Leaf>());
				}
				active.footer().numKVs = 0;
				active.footer().sizeFree = BODY_SIZE - sizeof(footer_t);

				if(! oldUsed)
				{
					oldUsed = true;
				}
				else
				{
					split->addSplit(key, active.pageId());
					pio->sync(active); // FIXME: this assumes delayed sync
				}
			}
			else
			{
			#ifdef VERBOSE_SPLIT
				std::cout << "new dupkey leaf" << std::endl;
			#endif

				// switch to leaf (dupkey leaf)
				DupKeyLeaf dl;
				if(! oldUsed)
				{
					dl = DupKeyLeaf(ovr, /* force = */ true);
					dl.hdr()->type = PT_DUPKEYLEAF;
				}
				else
				{
					dl = pio->newInitPage<DupKeyLeaf>();
				}

				dl.initBody(key);
				for(int j = iKeyS; j < iKeyE; ++ j)
				{
					dl.addValue(kvs[j].second);
				}

				if(! oldUsed)
				{
					oldUsed = true;	
				}
				else
				{
					split->addSplit(key, dl.pageId());
					pio->sync(dl);
				}

				iKeyS = iKeyE;
				continue;
			}

			iLeaf = 0;
		}

		{
			active.kv_offset(iLeaf++) = active.addKV(kvs[iKeyS]);
		}
		for(int j = iKeyS+1; j < iKeyE; ++ j)
		{
			active.kv_offset(iLeaf++) = active.addV(kvs[j].second) + VALUE_ONLY;
		}
		sizeFree = active.footer().sizeFree;

		iKeyS = iKeyE;
	}

	if(split->numSplit > 0)
	{
		split->pgidSplit = pageOrigId();
	}
#ifdef VERBOSE_SPLIT
	split->dump();
#endif
}

void
Leaf::dump_() const
{
	dumpHeader();

	std::cout << "  Leaf <numKVs: " << (size_t)footer().numKVs << ", sizeFree: " << footer().sizeFree << ">" << std::endl;
	Buffer k, v;
	btree_cursor_t cursor; cursor.leaf = *this;
	for(int i = 0; i < footer().numKVs; ++ i)
	{
		cursor.idx = i;
		cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cursor);

		uint16_t kvo = kv_offset(i);
		if(kvo & VALUE_ONLY)
		{
			std::cout << "  - " << kv_offset(i) - VALUE_ONLY << ":" << k << " : " << v << std::endl;
		}
		else
		{
			std::cout << "  * " << kv_offset(i) << ":" << k << " : " << v << std::endl;
		}
	}
	puts("");
}

void
Leaf::dumpGraph_(FILE* fp) const
{
	fprintf(fp, "\"page%u\" [\n", (unsigned int)pageId());
	fprintf(fp, "label = <<TABLE><TR>");
	fprintf(fp, "<TD PORT=\"head\">Leaf [%u]</TD>", (unsigned int)pageId());
	Buffer k, v;
	btree_cursor_t cursor; cursor.leaf = *this;
	for(int i = 0; i < footer().numKVs; ++ i)
	{
		cursor.idx = i;
		cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cursor);

		fprintf(fp, "<TD>%d</TD>", *(const int*)(k.get()));
	}
	fprintf(fp, "</TR></TABLE>>\nshape=plaintext\n");
	fprintf(fp, "];\n");
}

namespace 
{

void dkleaf_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ /* NOP */ }

void dkleaf_dump(const Page& pg, PageIO* pio)
{ DupKeyLeaf(pg).dump_(pio); }

void dkleaf_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ DupKeyLeaf(pg).dumpGraph_(fp, pio); }

static Page::dyndispatcher_t g_dkleaf_handlers = 
{
	dkleaf_updateLinks,
	dkleaf_dump,
	dkleaf_dumpGraph,
	NULL
};

Page::register_dyndispatcher g_dkleaf_reg(PT_DUPKEYLEAF, &g_dkleaf_handlers);

} // end of anonymous namespace

void
DupKeyLeaf::init(page_id_t id)
{
	initHdr(id, PT_DUPKEYLEAF);
}

void
DupKeyLeaf::initBody(BufferCRef key)
{
	footer().numVs = 0;
	footer().sizeFree = BODY_SIZE - sizeof(footer_t) - sizeof(/* sentinel */ uint16_t);

	if(! key.isValid())
	{
		footer().szKey = NOKEY_TAG;	
	}
	else if(PTNK_UNLIKELY(key.isNull()))
	{
		footer().szKey = NULL_TAG;
	}
	else
	{
		footer().szKey = key.size();
		char* p = rawbody() + BODY_SIZE - sizeof(footer_t) - footer().szKey;
		::memcpy(p, key.get(), key.size());

		footer().sizeFree -= footer().szKey;
	}

	uint16_t* sentinel = reinterpret_cast<uint16_t*>(rawbody());
	*sentinel = SENTINEL;
}

void
DupKeyLeaf::addValue(BufferCRef value)
{
	PTNK_ASSERT(value.isValid());

	char* p = rawbody() + BODY_SIZE - sizeof(footer_t) - footer().sizeFree - sizeof(/* sentinel */ uint16_t);
	if(! (footer().szKey & F_PSEUDOKEY)) p -= footer().szKey;

	uint16_t* szVal = reinterpret_cast<uint16_t*>(p);
	if(PTNK_UNLIKELY(value.isNull()))
	{
		*szVal = NULL_TAG;
		*(szVal+1) = SENTINEL;
	}
	else
	{
		*szVal = value.size(); p += sizeof(uint16_t);
		::memcpy(p, value.get(), value.size()); p += value.size();
		*reinterpret_cast<uint16_t*>(p) = SENTINEL;
	}

	footer().sizeFree -= sizeof(uint16_t) + *szVal;
	++ footer().numVs;
}

bool
DupKeyLeaf::insert(BufferCRef value, bool *bNotifyOldLink, PageIO* pio)
{
	PTNK_ASSERT(value.isValid());

	DupKeyLeaf ovr(pio->modifyPage(*this, bNotifyOldLink));

	size_t packedsz = value.packedsize() + sizeof(uint16_t);
	if(packedsz > footer().sizeFree)
	{
		return false;
	}

	ovr.addValue(value);

	pio->sync(ovr);

	return true;
}

bool
DupKeyLeaf::update(BufferCRef value, bool* bNotifyOldLink, PageIO* pio)
{
	PTNK_ASSERT(value.isValid());

	DupKeyLeaf ovr(pio->modifyPage(*this, bNotifyOldLink));

	PTNK_THROW_RUNTIME_ERR("FIXME (not implemented): DupKeyLeaf::update");

	pio->sync(ovr);
}

bool
DupKeyLeaf::popValue(ptnk::mod_info_t* mod, PageIO* pio)
{
	if(footer().numVs == 1) return false;

	DupKeyLeaf ovr(pio->modifyPage(*this, mod));

	size_t usedFront = BODY_SIZE - sizeof(footer_t) - footer().sizeFree;
	if(PTNK_LIKELY(footer().szKey != NULL_TAG)) usedFront -= footer().szKey;
	
	uint16_t packedsz = *reinterpret_cast<const uint16_t*>(rawbody());
	if(PTNK_UNLIKELY(packedsz == NULL_TAG)) packedsz = 0;
	packedsz += sizeof(uint16_t);

	::memmove(rawbody(), rawbody() + packedsz, usedFront - packedsz);
	
	-- footer().numVs;
	footer().sizeFree -= packedsz;

	pio->sync(ovr);

	return true;
}

DupKeyNode
DupKeyLeaf::makeTree(bool* bNotifyOldLink, PageIO* pio)
{
	// - clone old leaf and remove stored key from it
	DupKeyLeaf dlOld(pio->newInitPage<DupKeyLeaf>());
	{
		::memcpy(dlOld.rawbody(), rawbody(), BODY_SIZE);

		dlOld.removeKey();

		pio->sync(dlOld);
	}

	// make current leaf DupKeyNode as root
	DupKeyNode dnNewRoot(pio->modifyPage(*this, bNotifyOldLink), /* force = */ true);
	{
		dnNewRoot.hdr()->type = PT_DUPKEYNODE;
		dnNewRoot.initBody(key());

		// - add cloned old leaf as the first child
		dnNewRoot.addFirstChild(dlOld.pageId());

		pio->sync(dnNewRoot);
	}

	return dnNewRoot;
}

void
DupKeyLeaf::removeKey()
{
	footer_t& f = footer();

	const uint16_t packedszKey = f.szKey;
	if(! (packedszKey & F_PSEUDOKEY))
	{
		f.sizeFree += packedszKey;
	}
	f.szKey = NOKEY_TAG;
}

void
DupKeyLeaf::dump_(PageIO* pio) const
{
	dumpHeader();

	std::cout << "  DKLeaf <numVs: " << (size_t)footer().numVs << ", sizeFree: " << footer().sizeFree << ">" << std::endl;
	for(int i = 0; i < footer().numVs; ++ i)
	{
		std::cout << "  - " << v(i) << std::endl;
	}
	puts("");
}

void
DupKeyLeaf::dumpGraph_(FILE* fp, PageIO* pio) const
{
	// FIXME FIXME: implement DupKeyLeaf::dumpGraph_
#if 0
	fprintf(fp, "\"page%u\" [\n", (unsigned int)pageId());
	fprintf(fp, "label = <<TABLE><TR>");
	fprintf(fp, "<TD PORT=\"head\">Leaf [%u]</TD>", (unsigned int)pageId());
	for(uint8_t i = 0; i < footer().numKVs; ++ i)
	{
		fprintf(fp, "<TD>%d</TD>", *(const int*)(k(i).get()));
	}
	fprintf(fp, "</TR></TABLE>>\nshape=plaintext\n");
	fprintf(fp, "];\n");
#endif
}

namespace 
{

void dknode_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ DupKeyNode(pg).updateLinks_(mod, pio); }

void dknode_dump(const Page& pg, PageIO* pio)
{ DupKeyNode(pg).dump_(pio); }

void dknode_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ DupKeyNode(pg).dumpGraph_(fp, pio); }

bool dknode_refreshAllLeafPages(const Page& pg, void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio)
{ return DupKeyNode(pg).refreshAllLeafPages_(cursor, threshold, numPages, pgidDep, pio); }

static Page::dyndispatcher_t g_dknode_handlers = 
{
	dknode_updateLinks,
	dknode_dump,
	dknode_dumpGraph,
	dknode_refreshAllLeafPages
};

Page::register_dyndispatcher g_dknode_reg(PT_DUPKEYNODE, &g_dknode_handlers);

} // end of anonymous namespace

void
DupKeyNode::init(page_id_t id)
{
	initHdr(id, TYPE);
}

void
DupKeyNode::initBody(BufferCRef key)
{
	header().nPtr = 0;
	size_t sizeFree = BODY_SIZE - sizeof(header_t);
	if(key.isValid())
	{
		// init DupKeyNode w/ key
		//   DupKeyNode contains key when it is a root node in the dktree
		//   the key is removed when it becomes non-root node (see removeKey())

		if(key.isNull())
		{
			header().szKey = NULL_TAG;
		}
		else
		{
			char* p = rawbody() + BODY_SIZE - key.size();
			::memcpy(p, key.get(), key.size());
			header().szKey = key.size();
			sizeFree -= key.size();
		}
	}
	else
	{
		header().szKey = NOKEY_TAG;	
	}
	header().nPtrMax = sizeFree / sizeof(entry_t);
	header().lvl = 0;
}

BufferCRef
DupKeyNode::key() const
{
	uint16_t packedszKey = header().szKey;

	if(PTNK_UNLIKELY(packedszKey == NOKEY_TAG))
	{
		// this is a non-root node
		return BufferCRef::INVALID_VAL;
	}
	else if(PTNK_UNLIKELY(packedszKey == NULL_TAG))
	{
		return BufferCRef::NULL_VAL;
	}
	else
	{
		return BufferCRef(rawbody() + BODY_SIZE - packedszKey, packedszKey);	
	}
}

page_id_t
DupKeyNode::ptrAfter(page_id_t p) const
{
	int i, iE = header().nPtr;
	for(i = 0; i < iE; ++ i)
	{
		if(e(i).ptr == p)
		{
			break;	
		}
	}
	if(i == iE)
	{
		PTNK_THROW_RUNTIME_ERR("specified ptr not found");
	}
	if(i == iE - 1)
	{
		// no ptr after
		return PGID_INVALID;
	}

	return e(i+1).ptr;
}

page_id_t
DupKeyNode::ptrBefore(page_id_t p) const
{
	if(p == e(0).ptr)
	{
		// no ptr before
		return PGID_INVALID;
	}

	int i, iE = header().nPtr;
	for(i = 1; i < iE; ++ i)
	{
		if(e(i).ptr == p)
		{
			return e(i-1).ptr;	
		}
	}
	PTNK_THROW_RUNTIME_ERR("specified ptr not found");
}

void
DupKeyNode::removeKey()
{
	header().szKey = NOKEY_TAG;
	header().nPtrMax = (BODY_SIZE - sizeof(header_t)) / sizeof(entry_t);
}

void
DupKeyNode::insert(BufferCRef value, bool *bNotifyOldLink, PageIO* pio)
{
	if(insertR(value, bNotifyOldLink, pio))
	{
		return;			
	}

	// perform root split
	// - copy old node and remove stored key from it
	DupKeyNode dnOld(pio->newInitPage<DupKeyNode>());
	{
		::memcpy(dnOld.rawbody(), rawbody(), BODY_SIZE);

		dnOld.removeKey();

		pio->sync(dnOld);
	}
	
	// - create new root node as ovr
	DupKeyNode dnNewRoot(pio->modifyPage(*this, bNotifyOldLink));
	{
		dnNewRoot.initBody(key());	
		dnNewRoot.header().lvl = header().lvl + 1;

		// - add cloned old root as the first child
		dnNewRoot.addFirstChild(dnOld.pageId());
		
		pio->sync(dnNewRoot);
	}

	bool bSuccess = dnNewRoot.insertR(value, bNotifyOldLink, pio);
	PTNK_ASSERT(bSuccess); (void)bSuccess; // above should always success
}

bool
DupKeyNode::insertR(BufferCRef value, bool* bNotifyOldLink, PageIO* pio)
{
	const int nPtr = header().nPtr;

	if(header().lvl > 0)
	{
		// * children of this node are DupKeyNodes
	
		// find child node w/ ptr free
		int iX = -1;
		for(int i = 0; i < nPtr; ++ i)
		{
			const entry_t& o = e(i);
			if(o.sizeFree == MOSTFREE_TAG)
			{
				iX = i;	
				continue;
			}

			if(o.sizeFree > 0) // node w/ lvl > 0 holds number of node ptrs free
			{
				DupKeyNode dn(pio->readPage(o.ptr));
				
				if(! dn.insertR(value, bNotifyOldLink, pio))
				{
					PTNK_THROW_RUNTIME_ERR("insert to free DupKeyNode failed!");
				}

				if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

				// update sizeFree
				DupKeyNode ovr(pio->modifyPage(*this, bNotifyOldLink));
				{
					ovr.e(i).sizeFree = dn.ptrsFree();

					pio->sync(ovr);
				}

				return true;
			}
		}
		PTNK_ASSERT_CMNT(iX != -1, "child w/ MOSTFREE_TAG not found");

		// try inserting to the node w/ most free ptrs in the list
		DupKeyNode dnMostFree(pio->readPage(e(iX).ptr));

		mod_info_t modChild;
		if(dnMostFree.insertR(value, bNotifyOldLink, pio))
		{
			// insert success...

			if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

			return true;
		}

		// add new node 
		if(static_cast<int>(header().nPtrMax) - nPtr > 0)
		{
			DupKeyNode dnNew(pio->newInitPage<DupKeyNode>());
			dnNew.initBody(BufferCRef::INVALID_VAL); // don't store key to non-root node

			bool bSuccess = dnNew.insertR(value, bNotifyOldLink, pio);
			PTNK_ASSERT(bSuccess); (void)bSuccess; // above should never fail

			if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

			DupKeyNode ovr(pio->modifyPage(*this, bNotifyOldLink));
			{
				ovr.e(iX).sizeFree = dnMostFree.ptrsFree();
				ovr.e(nPtr).ptr = dnNew.pageId();
				ovr.e(nPtr).sizeFree = MOSTFREE_TAG;
				++ ovr.header().nPtr;

				pio->sync(ovr);
			}

			return true;
		}

		return false;
	}
	else
	{
		// * children of this node are DupKeyLeafs

		unsigned int reqsize = value.packedsize() + sizeof(uint16_t);
		
		// find child leaf capable for holding the value
		int iX = -1, iSecondMostFree = -1;
		uint16_t szSecondMostFree = 0;
		for(int i = 0; i < nPtr; ++ i)
		{
			const entry_t& o = e(i);

			if(o.sizeFree == MOSTFREE_TAG)
			{
				// handle this leaf later
				iX = i;	
				continue;
			}

			if(o.sizeFree >= reqsize)
			{
				DupKeyLeaf dl(pio->readPage(o.ptr));

				bool bSuccess = dl.insert(value, bNotifyOldLink, pio);
				PTNK_ASSERT(bSuccess); (void)bSuccess; // above should never fail

				if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

				DupKeyNode ovr(pio->modifyPage(*this, bNotifyOldLink));
				{
					ovr.e(i).sizeFree = dl.sizeFree();

					pio->sync(ovr);
				}

				return true;
			}

			if(o.sizeFree > szSecondMostFree)
			{
				iSecondMostFree = i;
				szSecondMostFree = o.sizeFree;	
			}
		}
		PTNK_ASSERT_CMNT(iX != -1, "child w/ MOSTFREE_TAG not found");

		// try inserting to the leaf w/ most free space in the list
		DupKeyLeaf dlMostFree(pio->readPage(e(iX).ptr));

		mod_info_t modChild;
		if(dlMostFree.insert(value, bNotifyOldLink, pio))
		{
			if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

			// check if dlMostFree is still the most free node
			if(dlMostFree.sizeFree() < szSecondMostFree)
			{
				// dlMostFree is no longer the most free child...
				PTNK_ASSERT(iSecondMostFree != -1);

				DupKeyNode ovr(pio->modifyPage(*this, bNotifyOldLink));
				{
					ovr.e(iX).sizeFree = dlMostFree.sizeFree();
					ovr.e(iSecondMostFree).sizeFree = MOSTFREE_TAG;

					pio->sync(ovr);	
				}

			}
			
			return true;
		}

		// add new leaf
		if(header().nPtrMax - header().nPtr > 0)
		{
			DupKeyLeaf dlNew(pio->newInitPage<DupKeyLeaf>());
			dlNew.initBody(BufferCRef::INVALID_VAL);// don't store key to non-root leaf

			bool bSuccess = dlNew.insert(value, bNotifyOldLink, pio);
			PTNK_ASSERT(bSuccess); (void)bSuccess; // above should never fail

			if(*bNotifyOldLink) pio->notifyPageWOldLink(pageOrigId());

			DupKeyNode ovr(pio->modifyPage(*this, bNotifyOldLink));
			{
				ovr.e(iX).sizeFree = dlMostFree.sizeFree();

				ovr.e(nPtr).ptr = dlNew.pageId();
				ovr.e(nPtr).sizeFree = MOSTFREE_TAG;
				++ ovr.header().nPtr;

				pio->sync(ovr);
			}

			return true;
		}

		return false;
	}
}

void
DupKeyNode::updateLinks_(mod_info_t* mod, PageIO* pio)
{
	// clone old node (FIXME: do this only if ovr == this)
	char temp_buf[PTNK_PAGE_SIZE];
	::memcpy(temp_buf, getRaw(), PTNK_PAGE_SIZE);
	DupKeyNode temp(Page(temp_buf, false));

	bool upd = false;
	unsigned int nPtr = header().nPtr;
	for(unsigned int i = 0; i < nPtr; ++ i)
	{
		page_id_t idOld = e(i).ptr;
		page_id_t idNew = pio->updateLink(idOld);

		if(idNew != idOld) upd = true;
		temp.e(i).ptr = idNew;
	}

	if(upd)
	{
		DupKeyNode ovr(pio->modifyPage(*this, mod));
		::memcpy(ovr.rawbody(), temp.rawbody(), BODY_SIZE);

		pio->sync(ovr);
	}
}

void
DupKeyNode::dump_(PageIO* pio) const
{
	dumpHeader();
	std::cout << "- DupKeyNode <lvl: " << (int)header().lvl <<  " nPtr: " << header().nPtr << " / " << header().nPtrMax << ">" << std::endl;
	std::cout << "    key: " << key() << std::endl;

	unsigned int i, nPtr = header().nPtr;
	for(i = 0; i < nPtr; ++ i)
	{
		std::cout << "  - " << pgid2str(e(i).ptr) << " sizeFree: " << e(i).sizeFree << std::endl;
	}

	if(pio)
	{
		for(i = 0; i < nPtr; ++ i)
		{
			pio->readPage(e(i).ptr).dump(pio);
		}
	}
}

void
DupKeyNode::dumpGraph_(FILE* fp, PageIO* pio) const
{
	// FIXME: impl.
}

bool
DupKeyNode::refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	PTNK_CHECK(false); // TODO: implement here!
}

page_id_t
btree_init(PageIO* pio)
{
	Node firstRoot(pio->newInitPage<Node>());
	Leaf firstLeaf(pio->newInitPage<Leaf>());

	firstRoot.initBody(firstLeaf.pageId());

	pio->sync(firstRoot);
	pio->sync(firstLeaf);

	return firstRoot.pageId();
}

static
bool
btree_cursor_prevleaf(btree_cursor_t* cur, PageIO* pio)
{
	PTNK_ASSERT(cur->isValid());

	Page pg(cur->leaf);

	// traverse up the tree
	while(! cur->nodes.empty())
	{
		Node& n = cur->nodes.back();
		page_id_t prev = n.ptrBefore(pg.pageOrigId());

		if(PGID_INVALID != prev)
		{
			pg = pio->readPage(prev);
			goto FOUND_PREV;
		}
		else
		{
			pg = n;
			cur->nodes.pop_back();
		}
	}
	// not found
	cur->leaf = Page();
	return false;

	FOUND_PREV:;
	// traverse down the tree
	while(pg.pageType() == PT_NODE)
	{
		Node n(pg);
		cur->nodes.push_back(n);
		pg = pio->readPage(n.ptrBack());	
	}

	cur->leaf = pg;

	return true;
}

static
bool
btree_cursor_nextleaf(btree_cursor_t* cur, PageIO* pio)
{
	PTNK_ASSERT(cur->isValid());

	Page pg(cur->leaf);

	// traverse up the tree
	while(! cur->nodes.empty())
	{
		Node& n = cur->nodes.back();
		page_id_t next = n.ptrAfter(pg.pageOrigId());

		if(PGID_INVALID != next)
		{
			pg = pio->readPage(next);
			goto FOUND_NEXT;
		}
		else
		{
			pg = n;
			cur->nodes.pop_back();
		}
	}
	// not found
	cur->leaf = Page();
	return false;

	FOUND_NEXT:;
	// traverse down the tree
	while(pg.pageType() == PT_NODE)
	{
		Node n(pg);
		cur->nodes.push_back(n);
		pg = pio->readPage(n.ptrFront());
	}

	cur->leaf = pg;
	return true;
}

page_id_t
btree_cursor_root(btree_cursor_t* cur)
{
	return cur->nodes.front().pageOrigId();
}

void
btree_query(btree_cursor_t* cur, page_id_t pgidRoot, const query_t& query, PageIO* pio)
{
	PTNK_ASSERT(query.isValid());

	cur->reset();

	Page pg = pio->readPage(pgidRoot);
	cur->nodes.push_back(Node(pg));
	for(;;)
	{
		const Node& node = cur->nodes.back();
		page_id_t next = node.query(query);

		Page pgNext(pio->readPage(next));
		switch(pgNext.pageType())
		{
		case PT_NODE:
			cur->nodes.push_back(Node(pgNext));
			continue;

		case PT_LEAF:
			cur->leaf = pgNext;
			if(!(query.type & F_NOQUERYLEAF))
			{
				Leaf(cur->leaf).query(cur, query);
			}
			return;

		case PT_DUPKEYLEAF:
		case PT_DUPKEYNODE:
			cur->leaf = pgNext;
			cur->idx = btree_cursor_t::SEE_DUPKEY_OFFSET;
			if(!(query.type & F_NOQUERYLEAF))
			{
				if(PTNK_UNLIKELY(query.type == BACK))
				{
					dktree_cursor_back(cur, pio);	
				}
				else
				{
					dktree_cursor_front(cur, pio);
				}
			}
			return;
		
		default:
			PTNK_THROW_RUNTIME_ERR("non-btree node/leaf page found during btree traversal");
		}
	}
}

ssize_t
btree_get(page_id_t pgidRoot, BufferCRef key, BufferRef value, PageIO* pio)
{
	PTNK_PROBE(PTNK_BTREE_GET_START());

	query_t query;
	query.key = key;
	query.type = MATCH_EXACT;

	btree_cursor_t cur;
	btree_query(&cur, pgidRoot, query, pio);

	// obtain value from the leaf node
	// ssize_t ret = cur.leaf.cursorGetValue(value, cur);
	ssize_t ret = Leaf(cur.leaf).get(key, value); 

	PTNK_PROBE(PTNK_BTREE_GET_END());
	return ret;
}

void
dktree_insert_exactkey(btree_cursor_t* cur, BufferCRef value, bool* bNotifyOldLink, PageIO* pio)
{
	if(cur->leaf.pageType() == PT_DUPKEYLEAF)
	{
		DupKeyLeaf dl(cur->leaf);
		if(dl.insert(value, bNotifyOldLink, pio))
		{
			// no leaf overflow...

			return;	
		}
		else
		{
			// leaf overflow...
			
			// make DupKey tree
			DupKeyNode dn(dl.makeTree(bNotifyOldLink, pio));
			dn.insert(value, bNotifyOldLink, pio);
		}
	}
	else /* if PT_DUPKEYNODE */
	{
		PTNK_ASSERT(cur->leaf.pageType() == PT_DUPKEYNODE);

		DupKeyNode(cur->leaf).insert(value, bNotifyOldLink, pio);
	}
}

void
dktree_insert(btree_cursor_t* cur, BufferCRef key, BufferCRef value, btree_split_t* split, bool* bNotifyOldLink, PageIO* pio)
{
	BufferCRef keyDK;
	if(cur->leaf.pageType() == PT_DUPKEYLEAF)
	{
		keyDK = DupKeyLeaf(cur->leaf).key();
	}
	else /* if PT_DUPKEYNODE */
	{
		keyDK = DupKeyNode(cur->leaf).key();
	}

	int cmp = bufcmp(key, keyDK);
	if(cmp == 0)
	{
		dktree_insert_exactkey(cur, value, bNotifyOldLink, pio);
	}
	else if(PTNK_LIKELY(cmp > 0))
	{
		// dup key leaf cannot accept key != dl.key()

		// strategy 1: find leaf next to this and put kv there
		{
			btree_cursor_t curN = *cur;
			if(btree_cursor_nextleaf(&curN, pio)
			&& curN.leaf.pageType() == PT_LEAF)
			{
				*cur = curN;
				Leaf l(cur->leaf);
				bool bUpdateKey = (bufcmp(key, l.keyFirst()) < 0);
				l.insert(key, value, split, bNotifyOldLink, pio);
				if(bUpdateKey)
				{
					if(! split->isValid())
					{
						split->reset();
						split->pgidSplit = l.pageOrigId();
					}
					
					split->keyNew = key;
				}
				return;
			}
		}

		// the leaf next to this don't exist or is dup key leaf...

		// strategy 2: create new leaf next to DupKeyLeaf
		{
			split->reset();
			split->pgidSplit = cur->leaf.pageOrigId();
			
			Leaf ln(pio->newInitPage<Leaf>());
			{
				btree_split_t s; bool _;
				ln.insert(key, value, &s, &_, pio);
			}

			split->addSplit(key, ln.pageId());
		}
	}
	else // if (cmp < 0)
	{
		// dup key leaf cannot accept key != dl.key()

		// XXXXX strategy 1: find leaf next to this and put kv there
		// - if leaf prev exist, would not come here!
		
		// strategy 2: create new leaf
		{
			// split prev is not supported, so make cur->leaf new Leaf, and split orig DupKeyLeaf 
			//
			//      [DupKeyLeaf/Node]
			//              |
			//        .-----------. (split[0])
			//        |           |
			//   [new Leaf]  [DupKeyLeaf/Node (copied)]

			split->reset();

			// - copied DupKeyLeaf/Node
			Page copy; page_id_t pgidCopy;
			boost::tie(copy, pgidCopy) = pio->newPage();
			copy.initHdr(pgidCopy, cur->leaf.pageType());
			::memcpy(copy.rawbody(), cur->leaf.rawbody(), Page::BODY_SIZE);
			pio->sync(copy);

			split->addSplit(keyDK, copy.pageId());

			// - new Leaf (overwrite previous DupKeyLeaf/Node)
			Leaf ln(pio->modifyPage(cur->leaf, bNotifyOldLink), true);
			{
				ln.hdr()->type = PT_LEAF;
				ln.initBody();

				btree_split_t s; bool _;
				ln.insert(key, value, &s, &_, pio);
			}
			split->pgidSplit = cur->leaf.pageOrigId();
			split->keyNew = key;
		}
	}
}

void
dktree_update(btree_cursor_t* cur, BufferCRef value, bool* bNotifyOldLink, PageIO* pio)
{
	// tmp
	DupKeyLeaf(cur->leaf).update(value, bNotifyOldLink, pio);
}

static
void
dktree_cursor_front(btree_cursor_t* cur, PageIO* pio)
{
	cur->dknodes.clear();

	Page pg(cur->leaf);
	while(pg.pageType() == PT_DUPKEYNODE)
	{
		DupKeyNode dn(pg);
		cur->dknodes.push_back(dn);
		
		pg = pio->readPage(dn.ptrFront());
	}

	cur->dkleaf = DupKeyLeaf(pg);
	cur->dloffset = 0;
}

static
void
dktree_cursor_back(btree_cursor_t* cur, PageIO* pio)
{
	cur->dknodes.clear();

	Page pg(cur->leaf);
	while(pg.pageType() == PT_DUPKEYNODE)
	{
		DupKeyNode dn(pg);
		cur->dknodes.push_back(dn);
		
		pg = pio->readPage(dn.ptrBack());
	}

	cur->dkleaf = DupKeyLeaf(pg);
	cur->dloffset = cur->dkleaf.offsetBack();
}

static
bool
dktree_cursor_nextdkleaf(btree_cursor_t* cur, PageIO* pio)
{
	// normalize cursor to next dkleaf

	Page pg(cur->dkleaf);

	// -- traverse up the tree
	while(! cur->dknodes.empty())
	{
		DupKeyNode& dn = cur->dknodes.back();
		page_id_t prev = dn.ptrAfter(pg.pageOrigId());

		if(PGID_INVALID != prev)
		{
			pg = pio->readPage(prev);
			goto FOUND_NEXT;
		}
		
		pg = dn;
		cur->dknodes.pop_back();
	}
	return false;

	FOUND_NEXT:;
	// -- traverse down the tree
	while(pg.pageType() == PT_DUPKEYNODE)
	{
		DupKeyNode dn(pg);
		cur->dknodes.push_back(dn);
		pg = pio->readPage(dn.ptrFront());
	}

	cur->dkleaf = DupKeyLeaf(pg);
	return true;
}

static
bool
dktree_cursor_prevdkleaf(btree_cursor_t* cur, PageIO* pio)
{
	// normalize cursor to next dkleaf

	Page pg(cur->dkleaf);

	// -- traverse up the tree
	while(! cur->dknodes.empty())
	{
		DupKeyNode& dn = cur->dknodes.back();
		page_id_t prev = dn.ptrBefore(pg.pageOrigId());

		if(PGID_INVALID != prev)
		{
			pg = pio->readPage(prev);
			goto FOUND_NEXT;
		}
		
		pg = dn;
		cur->dknodes.pop_back();
	}
	return false;

	FOUND_NEXT:;
	// -- traverse down the tree
	while(pg.pageType() == PT_DUPKEYNODE)
	{
		DupKeyNode dn(pg);
		cur->dknodes.push_back(dn);
		pg = pio->readPage(dn.ptrBack());
	}

	cur->dkleaf = DupKeyLeaf(pg);
	return true;
}

// propagate
//  1. split: child leaf/node split
//  2. bPrevWasOvr: notification that child was updated by overlay page
// toward top of the btree
static
page_id_t
btree_propagate(btree_split_t& split, bool bPrevWasOvr, btree_cursor_t* cur, PageIO* pio)
{
	VNode::reverse_iterator itNodes = cur->nodes.rbegin(), itE = cur->nodes.rend();
	page_id_t pgidRoot = cur->nodes.front().pageOrigId();

	// propagate leaf/node split toward the tree root
	while(PTNK_UNLIKELY(split.isValid()))
	{
		// split occurred!

		if(itNodes != itE)
		{
			// insert new node/leaf into the node above
			Node node(*itNodes);

			*itNodes = node.handleChildSplit(&split, &bPrevWasOvr, pio);

			if(bPrevWasOvr)
			{
				pio->notifyPageWOldLink(node.pageOrigId());
				for(unsigned int is = 0; is < split.numSplit; ++ is)
				{
					pio->notifyPageWOldLink(split.split[is].pgid);
				}
			}

			++ itNodes;
		}
		else
		{
			// root node needs split -> deepen the tree
			PTNK_ASSERT(pgidRoot == split.pgidSplit)
			{
				std::cout << "pgidRoot: " << pgidRoot << std::endl;
			}

			// create new root node and swap
			Node newRoot(pio->newInitPage<Node>());
			newRoot.initBody(/* prev */ pgidRoot);

			newRoot.handleChildSplitNoSelfSplit(&split, &bPrevWasOvr, pio);

			pio->notifyPageWOldLink(/* prev */ pgidRoot);
			pio->notifyPageWOldLink(newRoot.pageId());

			cur->nodes.insert(cur->nodes.begin(), newRoot);

			pgidRoot = newRoot.pageId();
			break;
		}
	}

	if(PTNK_UNLIKELY(bPrevWasOvr))
	{
		while(itNodes != itE)
		{
			Page pgNode(*itNodes); ++ itNodes;

			pio->notifyPageWOldLink(pgNode.pageOrigId());
		}
	}

	return pgidRoot;
}

page_id_t
btree_put(page_id_t pgidRoot, BufferCRef key, BufferCRef value, put_mode_t mode, page_id_t pgidDep, PageIO* pio)
{
	PTNK_PROBE(PTNK_BTREE_PUT_START());

	query_t query;
	query.key = key;
	query.type = MATCH_EXACT_NOLEAF;

	// traverse node pages and find relavant leaf node
	btree_cursor_t cur;
	btree_query(&cur, pgidRoot, query, pio);

	bool bPrevWasOvr = false;
	btree_split_t split;
	if(cur.leaf.pageType() == PT_LEAF)
	{
		switch(mode)
		{
		case PUT_INSERT:
			Leaf(cur.leaf).insert(key, value, &split, &bPrevWasOvr, pio);
			break;

		case PUT_UPDATE:
			Leaf(cur.leaf).update(key, value, &split, &bPrevWasOvr, pio);
			break;

		case PUT_LEAVE_EXISTING:
			Leaf(cur.leaf).insert(key, value, &split, &bPrevWasOvr, pio, /* abort on existing */ true);
			break;

		default:
			PTNK_THROW_RUNTIME_ERR("unknown put_mode specified");
		}
	}
	else
	{
		switch(mode)
		{
		case PUT_INSERT:
			dktree_insert(&cur, key, value, &split, &bPrevWasOvr, pio);
			break;

		case PUT_UPDATE:
			dktree_update(&cur, value, &bPrevWasOvr, pio);
			break;

		case PUT_LEAVE_EXISTING:
			PTNK_THROW_RUNTIME_ERR("not yet impl.");
			break;

		default:
			PTNK_THROW_RUNTIME_ERR("unknown put_mode specified");
		}
	}

	pgidRoot = btree_propagate(split, bPrevWasOvr, &cur, pio);

	PTNK_PROBE(PTNK_BTREE_PUT_END());
	return pgidRoot;
}

page_id_t
btree_del(page_id_t pgidRoot, BufferCRef key, PageIO* pio)
{
	PTNK_PROBE(PTNK_BTREE_DEL_START());

	query_t query;
	query.key = key;
	query.type = MATCH_EXACT_NOLEAF;

	// traverse node pages and find relavant leaf node
	btree_cursor_t cur;
	btree_query(&cur, pgidRoot, query, pio);

	pgidRoot = btree_cursor_del(&cur, pio).second;

	PTNK_PROBE(PTNK_BTREE_DEL_END());
	return pgidRoot;
}

btree_cursor_t*
btree_cursor_new()
{
	return new btree_cursor_t;
}

void
btree_cursor_delete(btree_cursor_t* cur)
{
	delete cur;
}

void
btree_cursor_get(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, btree_cursor_t* cur, PageIO* pio)
{
	if(cur->idx != btree_cursor_t::SEE_DUPKEY_OFFSET)
	{
		Leaf(cur->leaf).cursorGet(key, szKey, value, szValue, *cur);
	}
	else
	{
		if(szKey)
		{
			if(cur->leaf.pageType() == PT_DUPKEYLEAF)
			{
				*szKey = bufcpy(key, DupKeyLeaf(cur->leaf).key());
			}
			else
			{
				PTNK_ASSERT(cur->leaf.pageType() == PT_DUPKEYNODE);
				*szKey = bufcpy(key, DupKeyNode(cur->leaf).key());
			}
		}

		if(szValue)
		{
			*szValue = bufcpy(value, cur->dkleaf.vByOffset(cur->dloffset));
		}
	}
}

page_id_t
btree_cursor_put(btree_cursor_t* cur, BufferCRef value, PageIO* pio)
{
	bool bPrevWasOvr = false;
	btree_split_t split;
	if(cur->idx != btree_cursor_t::SEE_DUPKEY_OFFSET)
	{
		Leaf(cur->leaf).cursorPut(cur, value, &split, &bPrevWasOvr, pio);
	}
	else
	{
		PTNK_THROW_RUNTIME_ERR("dupkey tree update not yet impl.");
	}

	page_id_t pgidRoot = btree_propagate(split, bPrevWasOvr, cur, pio);

	return pgidRoot;
}

pair<bool, page_id_t>
btree_cursor_del(btree_cursor_t* cur, PageIO* pio)
{
	bool bLeafRemoved = false;

	bool bPrevWasOvr = false;
	page_id_t pgidRemove;

	if(Leaf(cur->leaf).cursorDelete(cur, &bPrevWasOvr, pio))
	{
		// the leaf is kept
		pgidRemove = PGID_INVALID;
	}
	else
	{
		// the leaf is to be removed from tree
		pgidRemove = cur->leaf.pageOrigId();
		bLeafRemoved = true;
	}

	page_id_t pgidNextChild = PGID_INVALID;

	// propagate leaf/node delete toward the tree root
	while(PTNK_UNLIKELY(pgidRemove != PGID_INVALID))
	{
		if(! cur->nodes.empty())
		{
			// delete node/leaf from the node above
			Node& node(cur->nodes.back());

			page_id_t pgidNode = node.pageOrigId();
			node = node.handleChildDel(&pgidNextChild, pgidRemove, &bPrevWasOvr, pio);

			if(node.isValid())
			{
				// the node is kept
				pgidRemove = PGID_INVALID;
			}
			else
			{
				// the node is to be removed from tree
				pgidRemove = pgidNode;	

				cur->nodes.pop_back();
			}

			if(bPrevWasOvr)
			{
				pio->notifyPageWOldLink(node.pageOrigId());
			}
		}
		else
		{
			// all entries in the tree has been removed ...

			// create a new empty tree
			page_id_t pgidRoot = btree_init(pio);
			Node nRoot(pio->readPage(pgidRoot));

			cur->nodes.clear();
			cur->nodes.push_back(nRoot);

			break;
		}
	}

	if(PTNK_UNLIKELY(bPrevWasOvr))
	{
		VNode::const_reverse_iterator itNodes = cur->nodes.rbegin(), itE = cur->nodes.rend();
		for(; itNodes != itE; ++ itNodes)
		{
			Page pgNode(*itNodes);

			pio->notifyPageWOldLink(pgNode.pageOrigId());
		}
	}

	bool bNextExist;
	if(bLeafRemoved)
	{
		Page pg;
		if(pgidNextChild != PGID_INVALID)
		{
			pg = pio->readPage(pgidNextChild);
		}
		else
		{
			// traverse up the tree until valid next child is found
			pg = cur->nodes.back(); cur->nodes.pop_back();
			while(! cur->nodes.empty())
			{
				Node& n = cur->nodes.back(); // second to last node
				pgidNextChild = n.ptrAfter(pg.pageOrigId());

				if(PGID_INVALID != pgidNextChild)
				{
					goto FOUND_NEXT;
				}
				else
				{
					pg = n;
					cur->nodes.pop_back();
				}
			}
			// not found
			cur->leaf = Page();
			bNextExist = false;
			goto RET;

		FOUND_NEXT:;
			pg = pio->readPage(pgidNextChild);
		}

		// traverse down the tree to next leaf
		while(pg.pageType() == PT_NODE)
		{
			Node n(pg);
			cur->nodes.push_back(n);
			pg = pio->readPage(n.ptrFront()); // FIXME FIXME not front but n.ptrNext(oldchild)
		}
		cur->leaf = pg;

		// FIXME: dup code
		if(cur->leaf.pageType() == PT_LEAF)
		{
			cur->idx = 0; // first record in the leaf
		}
		else
		{
			cur->idx = btree_cursor_t::SEE_DUPKEY_OFFSET;
			dktree_cursor_front(cur, pio);
		}

		bNextExist = true;
	}
	else
	{
		// step to next leaf if the last record in the leaf was deleted
		bNextExist = btree_cursor_next(cur, pio, /* bNormalizeOnly = */ true);
	}

RET:
	return make_pair(
		bNextExist,	
		cur->nodes.front().pageOrigId()
		);
}

bool
btree_cursor_next(btree_cursor_t* cur, PageIO* pio, bool bNormalizeOnly)
{
	if(cur->idx != btree_cursor_t::SEE_DUPKEY_OFFSET)
	{
		// cur->leaf pointing to Leaf

		if(! bNormalizeOnly)
		{
			++ cur->idx;
		}

		if(cur->idx < Leaf(cur->leaf).numKVs())
		{
			// cur->idx seems valid. exit here
			return true;
		}
	}
	else
	{
		// active record in cur->dkleaf

		if(! bNormalizeOnly)
		{
			cur->dkleaf.vByOffsetAndNext(&cur->dloffset);
		}

		if(cur->dloffset >= 0)
		{
			// cur->dloffset seems valid. exit here
			return true;
		}
		else if(dktree_cursor_nextdkleaf(cur, pio)) // try advancing to next dkleaf in dktree
		{
			// next dkleaf exist...

			// set offset to first entry in the dkleaf
			cur->dloffset = 0;

			return true;
		}
	}

	// normalize the cursor pointing to the leaf just after current leaf
	if(! btree_cursor_nextleaf(cur, pio))
	{
		// no leaf exist after current leaf
		return false;	
	}

	if(cur->leaf.pageType() == PT_LEAF)
	{
		cur->idx = 0; // first record in the leaf
	}
	else
	{
		cur->idx = btree_cursor_t::SEE_DUPKEY_OFFSET;
		dktree_cursor_front(cur, pio);
	}

	return true;
}

bool
btree_cursor_prev(btree_cursor_t* cur, PageIO* pio)
{
	if(cur->idx != btree_cursor_t::SEE_DUPKEY_OFFSET)
	{
		// cur->leaf pointing to Leaf
		-- cur->idx;

		if(cur->idx >= 0)
		{
			// cur->idx seems valid. exit here
			return true;
		}
	}
	else
	{
		// active record in cur->dkleaf	
		
		cur->dkleaf.offsetPrev(&cur->dloffset);
		
		if(cur->dloffset >= 0)
		{
			// cur->dloffset seems valid. exit here
			return true;
		}
		else if(dktree_cursor_prevdkleaf(cur, pio)) // try prev dkleaf in dktree
		{
			// prev dkleaf exist...

			// set offset to last entry in the dkleaf
			cur->dloffset = cur->dkleaf.offsetBack();

			return true;
		}
	}

	// normalize the cursor pointing to the leaf just before current leaf
	if(! btree_cursor_prevleaf(cur, pio))
	{
		// no leaf exist after current leaf
		return false;
	}

	if(cur->leaf.pageType() == PT_LEAF)
	{
		cur->idx = Leaf(cur->leaf).numKVs() - 1; // last record in the leaf
	}
	else
	{
		cur->idx = btree_cursor_t::SEE_DUPKEY_OFFSET;
		dktree_cursor_back(cur, pio);
	}

	// btree_cursor_dump(cur, pio);

	return true;
}

bool
btree_cursor_valid(btree_cursor_t* cur)
{
	return cur->isValid();
}

void
btree_cursor_dump(btree_cursor_t* cur, PageIO* pio)
{
	std::cout << "* btree_cursor_t dump" << std::endl;
	std::cout << "idx: " << cur->idx << std::endl;
	cur->leaf.dump(pio);
}

} // end of namespace ptnk
