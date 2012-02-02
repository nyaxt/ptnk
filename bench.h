#ifndef _bench_h_
#define _bench_h_

#include "ptnk_config.h"
#include "ptnk/sysutils.h"
using ptnk::HighResTimeStamp;

#include <sstream>
#include <iostream>
#include <iomanip>

class Bench 
{
public:
	Bench(const std::string& benchname)
	:	m_benchname(benchname)
	{
		start();
	}

	Bench(const std::string& progname, const std::string& comment)
	{
		std::stringstream ss;
		ss << progname << " " << comment;

		m_benchname = ss.str();
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
		for(struct checkpoint& cp: m_cps)
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
