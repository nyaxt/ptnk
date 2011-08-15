#ifndef _ptnk_overvew_h_
#define _ptnk_overvew_h_

#include "toc.h"

namespace ptnk
{

class OverviewPage : public Page
{
public:
	enum {
		TYPE = PT_DB_OVERVIEW,
	};

	explicit OverviewPage(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == TYPE){ std::cerr << "pg type: " << (uint16_t)pg.pageType() << std::endl; } }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id)
	{
		initHdr(id, PT_DB_OVERVIEW);
		verLayout() = id;
		*reinterpret_cast<uint16_t*>(offsetEntries()) = OVV_DELIMITER;
	}

	void setTableRoot(BufferCRef tableid, page_id_t pgidRoot, bool* bOvr, PageIO* pio);
	page_id_t getTableRoot(BufferCRef tableid, uint16_t* offset = NULL) const;

	void setTableRoot(TableOffCache* cache, page_id_t pgidRoot, bool* bOvr, PageIO* pio);
	page_id_t getTableRoot(TableOffCache* cache) const;

	void setDefaultTableRoot(page_id_t pgidRoot, bool* bOvr, PageIO* pio);
	page_id_t getDefaultTableRoot() const;

	void dropTable(BufferCRef tableid, bool* bOvr, PageIO* pio);

	BufferCRef getTableName(int idx) const;

	void updateLinks_(mod_info_t* mod, PageIO* pio);
	void dump_(PageIO* pio = NULL) const;
	void dumpGraph_(FILE* fp, PageIO* pio = NULL) const;
	bool refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const;

private:	
	enum
	{
		OVV_DELIMITER = 0xffff	
	};

	struct RALPCursor
	{
		void* cursorTable;
	};

	page_id_t& verLayout()
	{
		return *reinterpret_cast<page_id_t*>(rawbody());
	}

	page_id_t verLayout() const
	{
		return const_cast<OverviewPage*>(this)->verLayout();
	}

	char* offsetEntries()
	{
		return rawbody() + sizeof(page_id_t);	
	}

	const char* offsetEntries() const
	{
		return rawbody() + sizeof(page_id_t);	
	}

	static void setTableRootOvr(OverviewPage ovr, BufferCRef tableid, page_id_t pgidRoot, uint16_t* offset = NULL);
	void incrementVersion();

	// |tableidlen1|tableiddata1|pgidRoot1|tableidlen2|tableiddata2|pgidRoot2|0xffff|
};

} // end of namespace ptnk

#endif // _ptnk_overvew_h_
