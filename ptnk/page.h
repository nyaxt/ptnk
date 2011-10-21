#ifndef _ptnk_page_h_
#define _ptnk_page_h_

#include "common.h"
#include "exceptions.h"
#include "types.h"

#include <set>
#include <vector>
#include <string>

namespace ptnk
{

//                          0123456789abcdef
#define PGID_LOCALID_MASK 0x000FFFFFFFFFFFFFULL
#define PGID_LOCALID(pgid) ((pgid) & PGID_LOCALID_MASK)
#define PGID_PARTID(pgid) ((pgid) >> 52)
#define PGID_PARTSTART(partid) (((page_id_t)(partid)) << 52)
#define PGID_PARTLOCAL(partid, localid) (PGID_PARTSTART(partid) | (localid & PGID_LOCALID_MASK))
std::string pgid2str(page_id_t pgid);

static const page_id_t PGID_INVALID = (page_id_t)(~0);
typedef std::set<page_id_t> Spage_id_t;
typedef std::vector<page_id_t> Vpage_id_t;

//! partition local page id type
typedef uint64_t local_pgid_t;
#define PTNK_LOCALID_INVALID ((local_pgid_t)~0ULL)

//! partition id type
typedef uint16_t part_id_t;
//    the max valid partid is 4094 = 0xFFE to avoid being PGID_INVALID
#define PTNK_PARTID_MAX 4094
#define PTNK_PARTID_INVALID ((part_id_t)~0)

//! transaction ID number
/*!
 *	transaction unique identifier.
 *	Note: The ID is in order of transaction commit. 
 */
typedef uint64_t tx_id_t;
static const tx_id_t TXID_INVALID = (tx_id_t)(~0);

typedef std::set<tx_id_t> Stx_id_t;

enum page_type_t_
{
	PT_INVALID = 0,

	//! B tree node
	PT_NODE,

	//! B tree leaf
	PT_LEAF,

	//! B tree node with same key records
	PT_DUPKEYNODE,

	//! B tree leaf with same key records
	PT_DUPKEYLEAF,

	//! db overview page
	PT_DB_OVERVIEW,

	//! streak only page
	PT_OVFLSTREAK,

	//! compaction map page
	PT_COMPMAP,

	PT_DEBUG,

	PT_MAX = 255,
};
typedef uint8_t page_type_t;

struct page_hdr_t
{
	//! page id
	page_id_t id;

	//! override target page id
	page_id_t idOvrTgt;

	//! transaction id the page is involved in
	tx_id_t txid;

	//! page type 
	page_type_t type;

	typedef uint8_t flags_t;
	enum
	{
		//! this page is valid page
		PF_VALID = 1 << 0,

		//! this page is at end of transaction
		PF_END_TX = 1 << 1,

		//! this transaction is rebase transaction (valid only w/ PF_END_TX)
		PF_TX_REBASE = 1 << 2,
	};
	flags_t flags;
} __attribute__((__packed__));

class PageIO;

#ifndef PTNK_STREAK_SIZE
#define PTNK_STREAK_SIZE 40
#endif
#ifndef PTNK_BODY_SIZE
#define PTNK_BODY_SIZE PTNK_PAGE_SIZE - sizeof(page_hdr_t) - PTNK_STREAK_SIZE
#endif

struct mod_info_t;

const static size_t PTNK_PAGE_SIZE = 4096;

class Page
{
public:
	enum
	{
		HEADER_SIZE = sizeof(page_hdr_t),
		BODY_SIZE = PTNK_BODY_SIZE,
		STREAK_SIZE = PTNK_STREAK_SIZE
	};

	Page()
	:	m_impl(0)
	{
		setIsBase();	
	}

	Page(char* offset, bool isMutable)
	:	m_impl((uintptr_t)offset)
	{
		if(isMutable) setMutable();	
		setIsBase();	
	}

	char* getRaw() const
	{
		return (char*)(m_impl & ~(uintptr_t)0xf);
	}

	void initHdr(page_id_t id, page_type_t type)
	{
		hdr()->id = id;
		hdr()->type = type;
		hdr()->idOvrTgt = PGID_INVALID;
		hdr()->flags = 0;
		// ::memset(streak(), 0, PTNK_STREAK_SIZE);
	}

	page_hdr_t* hdr()
	{
		return reinterpret_cast<page_hdr_t*>(getRaw());
	}

	const page_hdr_t* hdr() const
	{
		return reinterpret_cast<const page_hdr_t*>(getRaw());
	}

	char* rawbody()
	{
		return reinterpret_cast<char*>(reinterpret_cast<page_hdr_t*>(getRaw())+1);
	}

	const char* rawbody() const
	{
		return reinterpret_cast<const char*>(reinterpret_cast<const page_hdr_t*>(getRaw())+1);
	}

	char* streak()
	{
		return getRaw() + HEADER_SIZE + BODY_SIZE;
	}

	const char* streak() const
	{
		return getRaw() + HEADER_SIZE + BODY_SIZE;	
	}

	bool isValid() const
	{
		return getRaw() && hdr()->type != PT_INVALID;
	}

	bool isCommitted() const
	{
		// FIXME: add checksum check
		return isValid()
		    && (hdr()->flags & page_hdr_t::PF_VALID)
			&& (hdr()->txid != TXID_INVALID);
	}

	page_id_t pageId() const
	{
		return hdr()->id;
	}

	page_id_t pageOrigId() const
	{
		return (!isBase() && hdr()->idOvrTgt != PGID_INVALID) ? hdr()->idOvrTgt : hdr()->id;
	}

	page_type_t pageType() const
	{
		return hdr()->type;
	}

	page_id_t pageOvrTgt() const
	{
		return hdr()->idOvrTgt;
	}

	void setTxid(tx_id_t txid)
	{
		hdr()->txid = txid;
	}

	void makePageOvr(const Page& pg, page_id_t idNew)
	{
		PTNK_ASSERT(isMutable());

		::memcpy(getRaw() + sizeof(page_hdr_t), pg.getRaw() + sizeof(page_hdr_t), PTNK_PAGE_SIZE - sizeof(page_hdr_t));
		
		hdr()->id = idNew;
		hdr()->type = pg.hdr()->type;
		if(pg.isBase() || pg.hdr()->idOvrTgt == PGID_INVALID)
		{
			hdr()->idOvrTgt = pg.hdr()->id;
		}
		else
		{
			hdr()->idOvrTgt = pg.hdr()->idOvrTgt;
		}
	}

	bool isMutable() const
	{
		return m_impl & 0x4;
	}

	void setMutable()
	{
		m_impl |= 0x4;	
	}

	bool isBase() const
	{
		return m_impl & 0x8;
	}

	void setIsBase(bool b = true)
	{
		if(b) m_impl |= 0x8;
		else m_impl &= ~(uintptr_t)0x8;
	}

	void dumpHeader() const;

	// *** re-inventing dynamic dispatch. this is ugly

	struct dyndispatcher_t
	{
		void (*updateLinks)(const Page& pg, mod_info_t* mod, PageIO* pio);	

		void (*dump)(const Page& pg, PageIO* pio);
		void (*dumpGraph)(const Page& pg, FILE* fp, PageIO* pio);

		bool (*refreshAllLeafPages)(const Page& pg, void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio);
	};
	static dyndispatcher_t* ms_dyndispatch[PT_MAX+1];

	class register_dyndispatcher
	{
	public:
		register_dyndispatcher(page_type_t pt, dyndispatcher_t* handler)
		{
			ms_dyndispatch[pt] = handler;	
		}
	};

	void updateLinks(mod_info_t* mod, PageIO* pio)
	{
		ms_dyndispatch[pageType()]->updateLinks(*this, mod, pio);
	}

	void dump(PageIO* pio) const;
	void dumpGraph(FILE* fp, PageIO* pio) const;
	bool refreshAllLeafPages(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const;

private:
	uintptr_t m_impl;
};

typedef std::vector<Page> VPage;

class DebugPage : public Page
{
public:
	enum { TYPE = PT_DEBUG, };
	
	DebugPage() { /* NOP */ }

	explicit DebugPage(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == TYPE); }			
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id)
	{
		initHdr(id, TYPE);	
	}

	void set(char c, bool* bOvr, PageIO* pio);
	char get() { return *rawbody(); }
};

} // end of namespace ptnk

#endif // _ptnk_page_h_
