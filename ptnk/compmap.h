#ifndef _ptnk_compmap_h_
#define _ptnk_compmap_h_

#include "page.h"
#include "bitvector.h"

namespace ptnk
{

class CompMapPage : public Page
{
private:
	struct body_hdr_t
	{
		uint64_t offsetOrig;
		uint64_t offsetComp;
	} __attribute__((__packed__));

public:
	enum {
		TYPE = PT_COMPMAP,
		BV_LENGTH_BYTES = Page::BODY_SIZE - sizeof(body_hdr_t),	
		BV_LENGTH = BV_LENGTH_BYTES * 8,
	};

	explicit CompMapPage(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == TYPE); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id)
	{
		initHdr(id, PT_COMPMAP);
	}

	void initBody(uint64_t offsetOrig);
	void setOffsetComp(uint64_t offsetComp)
	{
		bodyhdr().offsetComp = offsetComp;	
	}

	void dump(PageIO* pio = NULL) const;
	void dumpGraph(FILE* fp, PageIO* pio = NULL) const;

	//! compute pgid offset of compacted pagespace
	local_pgid_t translate(local_pgid_t pgidOrig);

	//! mark that page _pgidOrig_ is apparent in compacted pagespace
	void mark(local_pgid_t pgidOrig);

	size_t calcOffset(local_pgid_t pgidOrig);

	size_t countMarks();

private:
	body_hdr_t& bodyhdr()
	{
		return *reinterpret_cast<body_hdr_t*>(rawbody());
	}

	const body_hdr_t& bodyhdr() const
	{
		return const_cast<CompMapPage*>(this)->bodyhdr();	
	}

	BitVector bv()
	{
		return BitVector(rawbody() + sizeof(body_hdr_t), BV_LENGTH);	
	}
};

//! Compaction map pages mgmt
/*!
 *	usage:
 *		initPages()
 *		mark() mark() mark()
 *		fillOffsets()
 *
 *		loadPages()
 *		translate()...
 */
class CompMap
{
public:
	CompMap();

	//! init CompMapPages and return last used page_id + 1
	page_id_t initPages(local_pgid_t numOrigPages, PageIO* pio);

	local_pgid_t translate(local_pgid_t pgidOrig, PageIO* pio);
	void mark(local_pgid_t pgidOrig, PageIO* pio);

	page_id_t calcPgidCompMap(local_pgid_t pgidOrig);

	void fillOffsetComps(PageIO* pio);

private:
	//! page id of the first CompMapPage
	page_id_t m_pgidoffCompMap;

	//! number of pages in original pagespace
	local_pgid_t m_numOrigPages;
};

} // end of namespace ptnk

#endif // _ptnk_compmap_h_
