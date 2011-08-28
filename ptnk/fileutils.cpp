#include "fileutils.h"
#include "exceptions.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

namespace ptnk
{

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
