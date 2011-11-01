#ifndef _ptnk_helperthr_h_
#define _ptnk_helperthr_h_

#include "common.h"
#include <thread>
#include <queue>
#include <functional>

namespace ptnk
{

class Helper
{
public:
	Helper();
	~Helper();
	
	typedef std::function<void ()> Job;
	void enq(Job job);

private:
	void thrmain();

	unique_ptr<std::thread> m_thr;
	bool m_isCancelled;

	std::mutex m_mtxJobs;
	std::condition_variable m_condJobArrival;
	std::queue<Job> m_jobq;
};

} // end of namespace ptnk

#endif // _ptnk_helperthr_h_
