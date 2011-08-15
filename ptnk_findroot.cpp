#include "ptnk/pageiomem.h"
#include "ptnk/btree_int.h"

using namespace ptnk;

int main(int argc, char* argv[])
{
	try
	{
		PageIOMem pio(argv[1], 0);
		page_id_t pgidTgt = strtol(argv[2], NULL, 16);

		const page_id_t pgidE = pio.getLastPgId();
		{
		FINDNEXT:
			{
				Page pg(pio.readPage(pgidTgt));
				pg.dump(&pio);
			}
			
			for(page_id_t pgid = 0; pgid <= pgidE; ++ pgid)
			{
				Page pg(pio.readPage(pgid));
				
				if(! pg.isCommitted()) continue;
				if(pg.pageType() != PT_NODE) continue;

				Node node(pg);
				if(node.contains_(pgidTgt))
				{
					pgidTgt = pgid;	
					goto FINDNEXT;
				}
			}
		}
	}
	catch(std::exception& e)
	{
		std::cerr << e.what() << std::endl;	
	}
	catch(...)
	{
		std::cerr << "unknown exception caught" << std::endl;	
	}

	return 0;
}
