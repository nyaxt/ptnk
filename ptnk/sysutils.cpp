#include "sysutils.h"
#include "exceptions.h"

#include <iostream>
#include <unistd.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <array>
#include <thread>

namespace ptnk
{

std::vector<MutexProf*> MutexProf::s_profs;

MutexProf::MutexProf(const char* strid)
:	m_totalWaitLock(0)
{
	while(*strid == '.' || *strid == '/') strid++;
	m_strid = strid;

	s_profs.push_back(this);	
}

void
MutexProf::dumpStat()
{
	if(m_totalWaitLock < NSEC_PER_MSEC)
	{
		std::cerr << m_strid << ":\t" << m_totalWaitLock/NSEC_PER_USEC << "us" << std::endl;
	}
	else
	{
		std::cerr << m_strid << ":\t" << m_totalWaitLock/NSEC_PER_MSEC << "ms" << std::endl;
	}
}

void
MutexProf::dumpStatAll()
{
	for(MutexProf* prof: s_profs)
	{
		prof->dumpStat();
	}
}

bool
ptr_valid(void* ptr)
{
	// cf. http://stackoverflow.com/questions/7134590/how-to-test-if-an-address-is-readable-in-linux-userspace-app

	unsigned char t;
	// FIXME below code assumes pagesize == 4096. use sysconf to obtain correct value
	if(::mincore((void*)((uintptr_t)ptr & ~(uintptr_t)0xfff), 0xfff, &t) == 0)
	{
		return true;
	}
	else
	{
		if(errno == ENOMEM) return false;

		PTNK_THROW_RUNTIME_ERR("mincore failed");
	}
}

bool
file_exists(const char* filename)
{
	int fd = ::open(filename, O_RDONLY);
	if(fd >= 0)
	{
		::close(fd);
		return true;
	}
	else
	{
		if(errno == ENOENT)
		{
			return false;
		}
		else
		{
			throw ptnk_syscall_error(__FILE__, __LINE__, "open", errno);	
		}
	}
}

bool
checkperm(const char* filename, int flags)
{
	int ret = ::open(filename, flags);
	if(ret >= 0)
	{
		::close(ret);
		return true;
	}
	else
	{
		return false;
	}
}

#ifdef PTNK_STAGEPROF

stage_t g_curr_stage_th[MAX_NUM_THRS];
__thread int g_thr_id;

int
stageprof_genthrid()
{
	static std::mutex s_mtx;
	static int s_thrid_next = 1;

	std::unique_lock<std::mutex> g(s_mtx);

	return s_thrid_next++;
}

std::unique_ptr<std::thread> g_thr_stageprof;
volatile bool g_stop_stageprof = 0;

struct stg_inst
{
	stage_t stg;
	HighResTimeStamp tsStart;	
};
typedef std::vector<stg_inst> Vstg_inst;
std::array<Vstg_inst, MAX_NUM_THRS> g_stgprofs;

// profiling thr main
void
stageprof_thr_main()
{
	while(! g_stop_stageprof)
	{
		HighResTimeStamp ts; ts.reset();
		
		stage_t curr_stage_ss[MAX_NUM_THRS];
		for(int i = 0; i < MAX_NUM_THRS; ++ i)
		{
			curr_stage_ss[i] = g_curr_stage_th[i];
		}

		for(int i = 0; i < MAX_NUM_THRS; ++ i)
		{
			Vstg_inst& vsi = g_stgprofs[i];
			const stage_t currstg = curr_stage_ss[i];

			if(currstg != vsi.back().stg)
			{
				vsi.push_back(stg_inst{currstg, ts});
			}
		}

		usleep(10);
	}
}

void
stageprof_init()
{
	// initial stg
	{
		HighResTimeStamp tsStart; tsStart.reset();
		for(Vstg_inst& e: g_stgprofs)
		{
			e.push_back(stg_inst{0, tsStart});
		}
	}

	g_thr_stageprof.reset(new std::thread(stageprof_thr_main));
}

void
stageprof_dump()
{
	HighResTimeStamp tsEnd; tsEnd.reset();
	g_stop_stageprof = true;
	g_thr_stageprof->join();

	FILE* fp = fopen("stgprof.dump", "w");
	fprintf(fp, "TSEND %lu\n", tsEnd.elapsed_ns(g_stgprofs[0].front().tsStart));
	for(int i = 0; i < MAX_NUM_THRS; ++ i)
	{
		const Vstg_inst& vsi = g_stgprofs[i];
		if(vsi.size() < 2) continue;

		fprintf(fp, "THREAD %d\n", i);

		HighResTimeStamp tsStart = vsi.front().tsStart;

		for(const stg_inst& si: vsi)
		{
			fprintf(fp, "%lu, %06x\n", si.tsStart.elapsed_ns(tsStart), si.stg);
		}
	}
	fclose(fp);
}

#endif

} // end of namespace ptnk
