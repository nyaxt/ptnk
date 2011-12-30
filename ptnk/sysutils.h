#ifndef _ptnk_sysutils_h_
#define _ptnk_sysutils_h_

#include "common.h"

#include <vector>

#include <time.h>
#include <sys/time.h>

#ifdef USE_MACH_ABSOLUTE_TIME
#include <mach/mach_time.h>
#endif

#ifndef NSEC_PER_SEC
# define NSEC_PER_SEC                   1000000000ULL
# define NSEC_PER_MSEC                  1000000ULL
# define NSEC_PER_USEC                  1000ULL
#endif

namespace ptnk
{

class HighResTimeStamp
{
public:
#ifdef USE_CLOCK_GETTIME
	void reset()
	{
		::clock_gettime(CLOCK_MONOTONIC, &m_impl);
	}

	unsigned long elapsed_ns(const HighResTimeStamp& start) const
	{
		unsigned long ret;

		ret = (unsigned long)(m_impl.tv_sec - start.m_impl.tv_sec)*NSEC_PER_SEC;
		ret += (unsigned long)(m_impl.tv_nsec - start.m_impl.tv_nsec);
		
		return ret;
	}
		
private:
	struct timespec m_impl;

#elif defined(USE_MACH_ABSOLUTE_TIME)
	void reset()
	{
		m_impl = ::mach_absolute_time();
	}
	
	unsigned long elapsed_ns(const HighResTimeStamp& start) const
	{
		mach_timebase_info_data_t base; ::mach_timebase_info(&base);
		return (m_impl - start.m_impl) * base.numer / base.denom;
	}

private:
	uint64_t m_impl;
#else // fallback to gettimeofday
	void reset()
	{
		::gettimeofday(&m_impl, NULL);			
	}

	unsigned long elapsed_ns(const HighResTimeStamp& start) const
	{
		unsigned long ret;

		ret = (unsigned long)(m_impl.tv_sec - start.m_impl.tv_sec)*NSEC_PER_SEC;
		ret += (unsigned long)(m_impl.tv_usec - start.m_impl.tv_usec)*NSEC_PER_USEC;
		
		return ret;
	}

private:
	struct timeval m_impl;
#endif
};

class MutexProf
{
public:
	MutexProf(const char* strid);

	void accumWaitLock(unsigned long elapsed)
	{
		m_totalWaitLock += elapsed;	
	}

	void dumpStat();

	static void dumpStatAll();
	
private:
	static std::vector<MutexProf*> s_profs;

	const char* m_strid;
	unsigned long m_totalWaitLock;
};

#ifdef PTNK_MUTEXPROF
#define MUTEXPROF_START(DESCSTR) \
	static MutexProf prof__(__FILE__ ":" PTNK_STRINGIFY(__LINE__) ":" DESCSTR); \
	HighResTimeStamp tsBefore, tsAfter; tsBefore.reset();

#define MUTEXPROF_END \
	tsAfter.reset(); \
	prof__.accumWaitLock(tsAfter.elapsed_ns(tsBefore));
#else
#define MUTEXPROF_START(DESCSTR)
#define MUTEXPROF_END
#endif

#ifdef PTNK_STAGEPROF
extern __thread int g_thr_id;
constexpr int MAX_NUM_THRS = 32;
typedef uint32_t stage_t;
extern stage_t g_curr_stage_th[MAX_NUM_THRS];
int stageprof_genthrid();
void stageprof_init();
void stageprof_dump();
#define STAGEPROF_STAGE(s) do { if(g_thr_id == 0) { g_thr_id = ptnk::stageprof_genthrid(); } ptnk::g_curr_stage_th[g_thr_id] = (s); } while(0)
#else
#define STAGEPROF_STAGE(s)
#define stageprof_init()
#define stageprof_dump()
#endif

bool ptr_valid(void* ptr);
bool file_exists(const char* sysname);
bool checkperm(const char* sysname, int flags);

} // end of namespace ptnk

#endif // _ptnk_sysutils_h_
