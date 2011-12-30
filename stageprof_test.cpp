#include "ptnk/sysutils.h"

#include <iostream>
#include <thread>

using namespace ptnk;

typedef std::unique_ptr<std::thread> Pthread;

int
main()
{
	stageprof_init();

	std::vector<Pthread> thrs;

	thrs.push_back(Pthread(new std::thread([](){
		STAGEPROF_STAGE(0xff0000);
		usleep(100);
		STAGEPROF_STAGE(0x00ff00);
		usleep(100);
		STAGEPROF_STAGE(0x0000ff);
		usleep(100);
	})));

	thrs.push_back(Pthread(new std::thread([](){
		STAGEPROF_STAGE(0x0000ff);
		usleep(100);
		STAGEPROF_STAGE(0x00ff00);
		usleep(100);
		STAGEPROF_STAGE(0xff0000);
		usleep(100);
	})));

	sleep(1);
	stageprof_dump();

	for(const Pthread& t: thrs) t->join();	

	return 0;
}
