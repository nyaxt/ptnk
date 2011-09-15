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

#include <boost/foreach.hpp>

namespace ptnk
{

std::vector<MutexProf*> MutexProf::s_profs;

MutexProf::MutexProf(const char* strid)
:	m_strid(strid),
	m_totalWaitLock(0)
{
	s_profs.push_back(this);	
}

void
MutexProf::dumpStat()
{
	std::cerr << m_strid << ":\t" << m_totalWaitLock/NSEC_PER_USEC << "us" << std::endl;
}

void
MutexProf::dumpStatAll()
{
	BOOST_FOREACH(MutexProf* prof, s_profs)
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

} // end of namespace ptnk
