#include "ptnk/partitionedpageio.h"
#include <string>
#include <thread>
#include <vector>

using namespace ptnk;

int
main(int argc, char* argv[])
{
	if(argc < 3) return -1;

	int nthr = std::atoi(argv[1]);
	std::cout << "spawning " << nthr << " threads" << std::endl << std::flush;

	const char* dbname = argv[2];
	unique_ptr<PartitionedPageIO> pio(new PartitionedPageIO(dbname, ODEFAULT, 0644));

	typedef unique_ptr<std::thread> Uthread;
	typedef std::vector<Uthread> VUthread;

	VUthread thrs;
	for(int i = 0; i < nthr; ++ i)
	{
		thrs.push_back(Uthread(new std::thread([&pio, i]() {
			for(int j = 0; j < 10000; ++ j)
			{
				pio->newPage();	
			}
		})));
	}
	for(auto& pthr: thrs) { pthr->join(); }

	return 0;
}
