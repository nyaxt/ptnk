#ifndef _ptnk_hash_h_
#define _ptnk_hash_h_

#include <stdint.h>

namespace ptnk
{

uint64_t hash(const void* key, int len, unsigned int seed = 0);
uint64_t hashs(uint64_t key, unsigned int seed = 0);

};

#endif // _ptnk_hash_h_
