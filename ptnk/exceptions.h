#ifndef _ptnk_exceptions_h_
#define _ptnk_exceptions_h_

#include <string.h> // for strerror

#include <stdexcept>
#include <boost/format.hpp>

namespace ptnk
{

class ptnk_runtime_error : public std::runtime_error
{
public:
	explicit ptnk_runtime_error(const std::string& what)
	: std::runtime_error(what)
	{ /* NOP */ }

	explicit ptnk_runtime_error(const char* file, int line, const std::string& what)
	: std::runtime_error((boost::format("%1%:%2% %3%") % file % line % what).str())
	{ /* NOP */ }
};
#define PTNK_THROW_RUNTIME_ERR(x) throw ptnk_runtime_error(__FILE__, __LINE__, x);

class ptnk_logic_error : public std::logic_error 
{
public:
	explicit ptnk_logic_error(const char* file, int line, const std::string& what)
	: std::logic_error((boost::format("%1%:%2% %3%") % file % line % what).str())
	{ /* NOP */ }
};
#define PTNK_THROW_LOGIC_ERR(x) throw ptnk_logic_error(__FILE__, __LINE__, x);

class ptnk_syscall_error : public ptnk_runtime_error
{
public: 
	explicit ptnk_syscall_error(const char* file, int line, const std::string& syscall_name, int errn)
	: ptnk_runtime_error((boost::format("%1%:%2% %3% errno: %4% errdesc: %5%") % file % line % syscall_name % errn % strerror(errn)).str())
	{ /* NOP */ }
};
#define PTNK_ASSURE_SYSCALL(x) for(;(x) < 0; throw ptnk::ptnk_syscall_error(__FILE__, __LINE__, #x, errno))
#define PTNK_ASSURE_SYSCALL_NEQ(x, ERRCODE) for(;(x) == ERRCODE; throw ptnk::ptnk_syscall_error(__FILE__, __LINE__, #x, errno))

#define PTNK_CHECK(x) for(;!(x); throw ptnk::ptnk_logic_error(__FILE__, __LINE__, "assert failed: " #x))
#define PTNK_CHECK_CMNT(x, comment) for(;!(x); throw ptnk::ptnk_logic_error(__FILE__, __LINE__, "assert failed: " #x " comment: " comment))

#ifdef PTNK_DEBUG
#define PTNK_ASSERT(x) PTNK_CHECK(x)
#define PTNK_ASSERT_CMNT(x, comment) PTNK_CHECK_CMNT(x, comment)
#else
#define PTNK_ASSERT(x) if(false)
#define PTNK_ASSERT_CMNT(x, comment) if(false)
#endif

class ptnk_duplicate_key_error : public ptnk_runtime_error
{
public:
	explicit ptnk_duplicate_key_error()
	: ptnk_runtime_error("record for specified key already exist")
	{ /* NOP */ }
};

}

#endif // _ptnk_exceptions_h_
