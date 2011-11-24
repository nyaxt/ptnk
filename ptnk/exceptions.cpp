#include "exceptions.h"

#include <string.h> // for strerror
#include <sstream>

namespace ptnk
{

std::string
format_filelinewhat(const char* file, int line, const std::string& what)
{
	std::stringstream ret;

	ret << file << ":" << line << " " << what;

	return ret.str();
}

std::string
format_filelinesyscallerrn(const char* file, int line, const std::string& syscall_name, int errn)
{
	std::stringstream ret;

	ret << file << ":" << line << " " << syscall_name << " errno: " << errn << " " << ::strerror(errn);

	return ret.str();
}

} // end of namespace ptnk
