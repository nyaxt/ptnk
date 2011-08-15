#include "ptnk/pageiomem.h"
#include "ptnk/streak.h"

using namespace ptnk;

int main(int argc, char* argv[])
{
	try
	{
		PageIOMem pio(argv[1], 0);

		tx_id_t txid = atoi(argv[2]);
		Buffer bufStreakReal; bufStreakReal.reset();

		page_id_t pgidE = pio.getLastPgId();
		for(page_id_t pgid = 0; pgid <= pgidE; ++ pgid)
		{
			Page pg(pio.readPage(pgid));
			if(pg.isCommitted() && pg.hdr()->txid == txid)
			{
				if(pg.pageType() != PT_OVFLSTREAK)
				{
					BufferCRef pgstreak(pg.streak(), Page::STREAK_SIZE);
					// std::cout << "streak: " << pgstreak.hexdump() << std::endl;
					bufStreakReal.append(pgstreak);
				}
				else
				{
					OverflowedStreakPage ospg(pg);
					bufStreakReal.append(ospg.read());
				}
			}
		}

		BufferCRef bufStreak = bufStreakReal.rref();
		size_t count = *(size_t*)bufStreak.popFront(sizeof(size_t));
		// std::cout << "buf streak " << bufStreak.rref().hexdump() << std::endl;
		std::cout << "streak pol count " << count << std::endl;

		for(size_t i = 0; i < count; ++ i)
		{
			page_id_t pgid;
			bufStreak.popFrontTo(&pgid, sizeof(page_id_t));
			std::cout << "pgid: " << pgid2str(pgid) << std::endl;
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
