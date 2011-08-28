#ifndef _ptnk_sysutils_h_
#define _ptnk_sysutils_h_

namespace ptnk
{

bool ptr_valid(void* ptr);
bool file_exists(const char* sysname);
bool checkperm(const char* sysname, int flags);

} // end of namespace ptnk

#endif // _ptnk_sysutils_h_
