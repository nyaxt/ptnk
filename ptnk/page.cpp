#include "page.h"

#include "pageio.h" // for DebugPage::set

#include <iostream>

namespace ptnk
{

std::string
pgid2str(page_id_t pgid)
{
	if(pgid == PGID_INVALID)
	{
		return "INVALID";
	}

	char buf[20];
	sprintf(buf, "%x:%03llu", (unsigned int)PGID_PARTID(pgid), PGID_LOCALID(pgid));
	
	return buf;
}

Page::dyndispatcher_t* Page::ms_dyndispatch[PT_MAX+1];

void
Page::dumpHeader() const
{
	std::cout << "Page [id:" << pgid2str(hdr()->id) << " -> ovr:" << pgid2str(hdr()->idOvrTgt) << "] type: " << (uint16_t)(hdr()->type) << " txid: " << hdr()->txid << " flags: " << (uint16_t)hdr()->flags << std::endl;
}

void
Page::dump(PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];
	if(!table || !table->dump)
	{
		dumpHeader();
		std::cout << "** unknown pagetype" << std::endl;
	}
	else
	{
		table->dump(*this, pio);
	}
}

void
Page::dumpGraph(FILE* fp, PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];
	if(!table || !table->dumpGraph)
	{
		dumpHeader();
		std::cout << "** unknown pagetype" << std::endl;
	}
	else
	{
		table->dumpGraph(*this, fp, pio);
	}
}

bool
Page::refreshAllLeafPages(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	dyndispatcher_t* table = ms_dyndispatch[pageType()];

	PTNK_CHECK(table);
	PTNK_CHECK(table->refreshAllLeafPages);

	return table->refreshAllLeafPages(*this, cursor, threshold, numPages, pgidDep, pio);
}

void
DebugPage::set(char c, bool* bOvr, PageIO* pio)
{
	DebugPage ovr(pio->modifyPage(*this, bOvr));

	*ovr.rawbody() = c;

	pio->sync(ovr);
}

namespace 
{

void bt_updateLinks(const Page& pg, mod_info_t* mod, PageIO* pio)
{ BinTreePage(pg).updateLinks_(mod, pio); }

void bt_dump(const Page& pg, PageIO* pio)
{ BinTreePage(pg).dump_(pio); }

void bt_dumpGraph(const Page& pg, FILE* fp, PageIO* pio)
{ BinTreePage(pg).dumpGraph_(fp, pio); }

bool bt_refreshAllLeafPages(const Page& pg, void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio)
{ return BinTreePage(pg).refreshAllLeafPages_(cursor, threshold, numPages, pgidDep, pio); }

static Page::dyndispatcher_t g_bt_handlers = 
{
	bt_updateLinks,
	bt_dump,
	bt_dumpGraph,
	bt_refreshAllLeafPages,
};
Page::register_dyndispatcher g_bt_reg(PT_DEBUG_BINARYTREE, &g_bt_handlers);

} // end of anonymous namespace

struct BinTreePage::Layout
{
	char c;
	page_id_t child[2];
};

void
BinTreePage::set(char c, page_id_t pgidA, page_id_t pgidB, bool* bOvr, PageIO* pio)
{
	BinTreePage ovr(pio->modifyPage(*this, bOvr));

	Layout* body = reinterpret_cast<Layout*>(ovr.rawbody());
	body->c = c;
	body->child[0] = pgidA;
	body->child[1] = pgidB;

	pio->sync(ovr);
}

void
BinTreePage::updateLinks_(mod_info_t* mod, PageIO* pio)
{
	const Layout* orig = reinterpret_cast<const Layout*>(rawbody());
	if(orig->child[0] == PGID_INVALID) return; // return if leaf 

	page_id_t pgidNewA = pio->updateLink(orig->child[0]);
	page_id_t pgidNewB = pio->updateLink(orig->child[1]);

	if(pgidNewA != orig->child[0] || pgidNewB != orig->child[1])
	{
		// set(orig->c, pgidNewA, pgidNewB, bOvr, pio);
		BinTreePage ovr(pio->modifyPage(*this, mod));

		Layout* body = reinterpret_cast<Layout*>(ovr.rawbody());
		body->c = orig->c;
		body->child[0] = pgidNewA;
		body->child[1] = pgidNewB;

		pio->sync(ovr);
	}
}

void
BinTreePage::dump_(PageIO* pio) const
{
	dumpHeader();

	const Layout* body = reinterpret_cast<const Layout*>(rawbody());
	std::cout << "- BinTreePage: [" << body->c << "]" << std::endl;
	std::cout << "    child[0]: " << body->child[0] 
	          << "    child[1]: " << body->child[1]
	          << std::endl;
	
	if(pio && body->child[0] != PGID_INVALID)
	{
		pio->readPage(body->child[0]).dump(pio);	
		pio->readPage(body->child[1]).dump(pio);	
	}
}

void
BinTreePage::dumpGraph_(FILE* fp, PageIO* pio) const
{
	const Layout* body = reinterpret_cast<const Layout*>(rawbody());

	fprintf(fp, "\"page%u\" [\n", (unsigned int)pageId());
	std::string strPageId = pgid2str(pageId());
	std::string strPageOvrTgt = pgid2str(pageId());
	fprintf(fp, "label = \"[id: %s ovr: %s]:%c\"", strPageId.c_str(), strPageOvrTgt.c_str(), body->c);
	fprintf(fp, "];\n");
	
	if(pio && body->child[0] != PGID_INVALID)
	{
		pio->readPage(body->child[0]).dumpGraph(fp, pio);
		pio->readPage(body->child[1]).dumpGraph(fp, pio);

		fprintf(fp, "\"page%u\" -> \"page%u\";\n", (unsigned int)pageId(), (unsigned int)body->child[0]);
		fprintf(fp, "\"page%u\" -> \"page%u\";\n", (unsigned int)pageId(), (unsigned int)body->child[1]);
	}
}

bool
BinTreePage::refreshAllLeafPages_(void** cursor, page_id_t threshold, int numPages, page_id_t pgidDep, PageIO* pio) const
{
	const Layout* body = reinterpret_cast<const Layout*>(rawbody());
	if(body->child[0] == PGID_INVALID)
	{
		// bintree leaf

		if(pageId() < threshold)
		{
			// re-write this page to the log
			BinTreePage refresh(pio->modifyPage(*this));
			pio->sync(refresh);

			return true;
		}
		else
		{
			return false;
		}
	}
	else
	{
		// bintree node

		bool bProp = false; // laterly set true if any obsolete leaf has been found

		for(page_id_t pgidChild: body->child)
		{
			if(pio->readPage(pgidChild).refreshAllLeafPages(NULL, threshold, numPages, pgidDep, pio))
			{
				bProp = true;	
			}
		}
		if(bProp)
		{
			pio->notifyPageWOldLink(pageOrigId());
		}

		return bProp;
	}
}

} // end of namespace ptnk
