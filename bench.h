#ifndef _bench_h_
#define _bench_h_

#include "ptnk_config.h"

#include <iostream>
#include <iomanip>
#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <time.h>
#include <sys/time.h>

#ifdef USE_MACH_ABSOLUTE_TIME
#include <mach/mach_time.h>
#endif

#ifndef NSEC_PER_SEC
# define NSEC_PER_SEC                   1000000000ULL
# define NSEC_PER_USEC                  1000ULL
#endif

class HighResTimeStamp
{
public:
	HighResTimeStamp()
	{
		reset();
	}

#ifdef USE_CLOCK_GETTIME
	void reset()
	{
		::clock_gettime(CLOCK_MONOTONIC, &m_impl);
	}

	unsigned long elapsed_ns(const HighResTimeStamp& start)
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
	
	unsigned long elapsed_ns(const HighResTimeStamp& start)
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

	unsigned long elapsed_ns(const HighResTimeStamp& start)
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


class Bench 
{
public:
	Bench(const std::string& benchname)
	:	m_benchname(benchname)
	{
		start();
	}

	void start()
	{
		m_cps.clear();
		m_ts_start.reset();
	}

	void end()
	{
		m_ts_end.reset();
	}

	void cp(const char* cpname)
	{
		struct checkpoint cp;
		cp.name = cpname;
		cp.ts.reset();
		m_cps.push_back(cp);
	}

	void dump()
	{
		std::cout << "RESULT\t" << m_benchname << '\t' << std::setiosflags(std::ios::fixed) << std::setprecision(4) << (double)m_ts_end.elapsed_ns(m_ts_start) / NSEC_PER_SEC;
		BOOST_FOREACH(struct checkpoint& cp, m_cps)
		{
			std::cout << '\t' << std::setiosflags(std::ios::fixed) << std::setprecision(4) << (double)cp.ts.elapsed_ns(m_ts_start) / NSEC_PER_SEC;
		}
		std::cout << std::endl;
	}

private:
	std::string m_benchname;
	
	HighResTimeStamp m_ts_start, m_ts_end;
	struct checkpoint
	{
		HighResTimeStamp ts;
		const char* name;
	};
	std::vector<checkpoint> m_cps;
};

#endif // _bench_h_
