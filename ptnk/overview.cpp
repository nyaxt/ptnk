#include "overview.h"
#include "pageio.h"

namespace ptnk
{

namespace 
{

void ovv_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ OverviewPage(pg).updateLinks_(mod, pio); }

void ovv_dump(const Page& pg, PageIO* pio)
{ OverviewPage(pg).dump_(pio); }

void ovv_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ OverviewPage(pg).dumpGraph_(fp, pio); }

bool ovv_refreshAllLeafPages(const Page& pg, void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio)
{ return OverviewPage(pg).refreshAllLeafPages_(cursor, threshold, numPages, pgidDep, pio); }
	
static Page::dyndispatcher_t g_ovv_handlers = 
{
	ovv_updateLinks,
	ovv_dump,
	ovv_dumpGraph,
	ovv_refreshAllLeafPages,
};

Page::register_dyndispatcher g_ovv_reg(PT_DB_OVERVIEW, &g_ovv_handlers);

} // end of anonymous namespace

void
OverviewPage::setTableRootOvr(OverviewPage ovr, BufferCRef tableid, page_id_t pgidRoot, uint16_t* offset)
{
	char* p = ovr.offsetEntries();
	for(;;)
	{
		PTNK_ASSERT(p - ovr.rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) break;

		PTNK_ASSERT(sizeId < BODY_SIZE);
		BufferCRef bufId(p + sizeof(uint16_t), sizeId);
		
		// table id found
		if(bufeq(bufId, tableid))
		{
			// update pgidRoot
			page_id_t* proot = reinterpret_cast<page_id_t*>(p + sizeof(uint16_t) + sizeId);
			*proot = pgidRoot;

			if(offset)
			{
				*offset = p + sizeof(uint16_t) + sizeId - ovr.offsetEntries();	
			}
			return;
		}

		p += sizeof(uint16_t) + sizeId + sizeof(page_id_t);
	}

	// handle new table id
	{
		uint16_t* sizeId = reinterpret_cast<uint16_t*>(p);
		*sizeId = tableid.size();
		::memcpy(p + sizeof(uint16_t), tableid.get(), tableid.size());
		
		page_id_t* proot = reinterpret_cast<page_id_t*>(p + sizeof(uint16_t) + tableid.size());
		*proot = pgidRoot;
		if(offset)
		{
			*offset = p + sizeof(uint16_t) + tableid.size() - ovr.offsetEntries();
		}

		p += sizeof(uint16_t) + *sizeId + sizeof(page_id_t);
	}

	*reinterpret_cast<uint16_t*>(p) = OVV_DELIMITER;

	// update layout ver.
	ovr.incrementVersion();
}

void
OverviewPage::setTableRoot(BufferCRef tableid, page_id_t pgidRoot, bool* bOvr, PageIO* pio)
{
	PTNK_ASSERT(tableid.isValid());
	PTNK_ASSERT(! tableid.isNull());

	OverviewPage ovr(pio->modifyPage(*this, bOvr));

	setTableRootOvr(ovr, tableid, pgidRoot);

	pio->sync(ovr);
}

page_id_t
OverviewPage::getTableRoot(BufferCRef tableid, uint16_t* offset) const
{
	PTNK_ASSERT(tableid.isValid());
	PTNK_ASSERT(! tableid.isNull());

	const char* p = offsetEntries();
	
	for(;;)
	{
		PTNK_ASSERT(p - rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<const uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) break;

		PTNK_ASSERT(sizeId < BODY_SIZE);
		BufferCRef bufId(p + sizeof(uint16_t), sizeId);
		
		// table id found
		if(bufeq(bufId, tableid))
		{
			// return pgidRoot
			const page_id_t* proot = reinterpret_cast<const page_id_t*>(p + sizeof(uint16_t) + sizeId);
			if(offset)
			{
				*offset = p + sizeof(uint16_t) + sizeId - offsetEntries();
			}
			return *proot;
		}

		p += sizeof(uint16_t) + sizeId + sizeof(page_id_t);
	}
	
	// not found
	return PGID_INVALID;
}

// #define VERBOSE_CACHE

void
OverviewPage::setTableRoot(TableOffCache* cache, page_id_t pgidRoot, bool* bOvr, PageIO* pio)
{
	OverviewPage ovr(pio->modifyPage(*this, bOvr));

#ifdef VERBOSE_CACHE
	std::cerr << "set cached tableid: " << cache->getTableId() << " valid? : " << (verLayout() == cache->m_verLayout) << std::endl;
#endif
	if(verLayout() != cache->m_verLayout)
	{
		// cache invalid...

		setTableRootOvr(ovr, cache->getTableId(), pgidRoot, &cache->m_offset);
		cache->m_verLayout = ovr.verLayout();

		pio->sync(ovr);
	}
	else
	{
		// cache valid...

		*reinterpret_cast<page_id_t*>(ovr.offsetEntries() + cache->m_offset) = pgidRoot;

		pio->sync(ovr);
	}
}

page_id_t
OverviewPage::getTableRoot(TableOffCache* cache) const
{
#ifdef VERBOSE_CACHE
	std::cerr << "get cached tableid: " << cache->getTableId() << " valid? : " << (verLayout() == cache->m_verLayout) << std::endl;
#endif
	if(verLayout() != cache->m_verLayout)
	{
		// cache invalid...

		page_id_t ret = getTableRoot(cache->getTableId(), &cache->m_offset);
		if(ret != PGID_INVALID) cache->m_verLayout = verLayout();
		return ret;
	}
	else
	{
		// cache valid...
		
		return *reinterpret_cast<const page_id_t*>(offsetEntries() + cache->m_offset);
	}
}

void
OverviewPage::setDefaultTableRoot(page_id_t pgidRoot, bool* bOvr, PageIO* pio)
{
	OverviewPage ovr(pio->modifyPage(*this, bOvr));

	char* p = ovr.offsetEntries();
	{
		uint16_t sizeId = *reinterpret_cast<uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) PTNK_THROW_RUNTIME_ERR("no tables found in ovv page");

		PTNK_ASSERT(sizeId < BODY_SIZE);
		
		page_id_t* proot = reinterpret_cast<page_id_t*>(p + sizeof(uint16_t) + sizeId);
		*proot = pgidRoot;
	}
	
	pio->sync(ovr);
}

page_id_t
OverviewPage::getDefaultTableRoot() const
{
	const char* p = offsetEntries();
	
	{
		uint16_t sizeId = *reinterpret_cast<const uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) PTNK_THROW_RUNTIME_ERR("no tables found in ovv page");

		PTNK_ASSERT(sizeId < BODY_SIZE);
		BufferCRef bufId(p + sizeof(uint16_t), sizeId);
		
		const page_id_t* proot = reinterpret_cast<const page_id_t*>(p + sizeof(uint16_t) + sizeId);
		return *proot;
	}
}

void
OverviewPage::dropTable(BufferCRef tableid, bool* bOvr, PageIO* pio)
{
	OverviewPage ovr(pio->modifyPage(*this, bOvr));

	char* p = ovr.offsetEntries();
	for(;;)
	{
		PTNK_ASSERT(p - ovr.rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<uint16_t*>(p);
		if(sizeId == OVV_DELIMITER)
		{
			PTNK_THROW_RUNTIME_ERR("no such table found");
		}

		PTNK_ASSERT(sizeId < BODY_SIZE);
		BufferCRef bufId(p + sizeof(uint16_t), sizeId);

		const size_t szEntry = sizeof(uint16_t) + sizeId + sizeof(page_id_t);
		
		// table id found
		if(bufeq(bufId, tableid))
		{
			// delete the entry
			::memmove(p, p + szEntry, BODY_SIZE - (p + szEntry - ovr.rawbody()));

			// update layout ver.
			ovr.incrementVersion();

			pio->sync(ovr);

			return;
		}

		p += szEntry;
	}
	
	PTNK_ASSERT(false); // should not come here
}

BufferCRef
OverviewPage::getTableName(int idx) const
{
	const char* p = offsetEntries();
	for(int n = idx; n >= 0; -- n)
	{
		PTNK_ASSERT(p - rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<const uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) break;
		PTNK_ASSERT(sizeId < BODY_SIZE);

		if(n == 0)
		{
			return BufferCRef(p + sizeof(uint16_t), sizeId);
		}

		p += sizeof(uint16_t) + sizeId + sizeof(page_id_t);
	}

	return BufferCRef::INVALID_VAL;
}

void
OverviewPage::incrementVersion()
{
	// verLayout() = pageId(); // this would not work on non TPIO env.
	++ verLayout();
}

void
OverviewPage::updateLinks_(mod_info_t* mod, PageIO* pio)
{
	OverviewPage ovr(pio->modifyPage(*this, mod));

	char* p = ovr.offsetEntries();
	for(;;)
	{
		PTNK_ASSERT(p - ovr.rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) break;

		// update pgidRoot
		page_id_t* proot = reinterpret_cast<page_id_t*>(p + sizeof(uint16_t) + sizeId);
		*proot = pio->updateLink(*proot);

		p += sizeof(uint16_t) + sizeId + sizeof(page_id_t);
	}

	pio->sync(ovr);
}

void
OverviewPage::dump_(PageIO* pio) const
{
	dumpHeader();
	std::cout << "  OverviewPage <verLayout: " << pgid2str(verLayout()) << ">" << std::endl;

	const char* p = offsetEntries();
	for(;;)
	{
		PTNK_ASSERT(p - rawbody() < BODY_SIZE);

		uint16_t sizeId = *reinterpret_cast<const uint16_t*>(p);
		if(sizeId == OVV_DELIMITER) break;

		PTNK_ASSERT(sizeId < BODY_SIZE);
		BufferCRef bufId(p + sizeof(uint16_t), sizeId);

		const page_id_t* proot = reinterpret_cast<const page_id_t*>(p + sizeof(uint16_t) + sizeId);
		std::cout << "    Table: " << bufId << " root pgid: " << pgid2str(*proot) << std::endl;
		if(pio) pio->readPage(*proot).dump(pio);

		p += sizeof(uint16_t) + sizeId + sizeof(page_id_t);
	}
}

void
OverviewPage::dumpGraph_(FILE* fp, PageIO* pio) const
{
	// FIXME: dump other tables
	pio->readPage(getDefaultTableRoot()).dumpGraph(fp, pio);	
}

bool
OverviewPage::refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	PTNK_ASSERT(pgidDep == PGID_INVALID);

	if(*cursor && numPages == 0)
	{
		// free cursor
		RALPCursor* ralpc = static_cast<RALPCursor*>(*cursor);
		delete ralpc;

		*cursor = NULL;
	}

	RALPCursor* ralpc;
	if(*cursor)
	{
		ralpc = static_cast<RALPCursor*>(*cursor);
	}
	else
	{
		// new cursor
		*cursor = ralpc = new RALPCursor;
		ralpc->cursorTable = NULL;
	}

	if(pio->readPage(getDefaultTableRoot()).refreshAllLeafPages(&ralpc->cursorTable, threshold, numPages, pageOrigId(), pio))
	{
		pio->notifyPageWOldLink(pageOrigId());	
	}

	if(! ralpc->cursorTable)
	{
		// free cursor
		delete ralpc;

		*cursor = NULL;
	}

	return false; // does not matter
}

} // end of namespace ptnk
