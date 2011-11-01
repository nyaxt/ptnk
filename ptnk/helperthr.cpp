#include "helperthr.h"

namespace ptnk
{

Helper::Helper()
:	m_isCancelled(false)
{
	m_thr.reset(new std::thread([this]() { thrmain(); }));
}

Helper::~Helper()
{
	m_isCancelled = true;
	m_condJobArrival.notify_all();
	
	m_thr->join();
}

void
Helper::enq(Job job)
{
	std::unique_lock<std::mutex> g(m_mtxJobs);

	m_jobq.push(job);
	m_condJobArrival.notify_all();
}

void
Helper::thrmain()
{
	while(! m_isCancelled)
	{
		Job job; 

		// wait next job
		{
			std::unique_lock<std::mutex> g(m_mtxJobs);

			while(m_jobq.empty())
			{
				// jobq empty...

				// wait cond for new job arrival
				m_condJobArrival.wait_for(g, std::chrono::milliseconds(50));

				if(m_isCancelled) return;
			}

			std::swap(job, m_jobq.front());
			m_jobq.pop();
		}

		job();
	}
}

} // end of namespace ptnk
