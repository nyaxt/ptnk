#include "ptnk/pageiomem.h"

using namespace ptnk;

int main(int argc, char* argv[])
{
	try
	{
		PageIOMem pio(argv[1], 0);

		page_id_t pgidE = pio.getLastPgId();
		for(page_id_t pgid = 0; pgid <= pgidE; ++ pgid)
		{
			Page pg(pio.readPage(pgid));
			pg.dump(&pio);
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
