#ifndef _ptnk_fileutils_h_
#define _ptnk_fileutils_h_

namespace ptnk
{

bool file_exists(const char* filename);
bool checkperm(const char* filename, int flags);

} // end of namespace ptnk

#endif // _ptnk_fileutils_h_
