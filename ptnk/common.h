#ifndef _ptnk_common_h_
#define _ptnk_common_h_

#ifdef HAVE_PTNK_CONFIG_H

#include "ptnk_config.h"
#ifdef HAVE_DTRACE
#include "ptnk_probes.h"
#define PTNK_PROBE(x) x
#else
#define PTNK_PROBE(x)
#endif

#endif /* HAVE_PTNK_CONFIG_H */

/* to be included on all ptnk source files AND ptnk C/C++ API */

#include <stdint.h>
#include <string.h>

#ifndef PTNK_DEBUG
#define BOOST_DISABLE_ASSERTS
#endif

#ifdef __GNUC__
#define PTNK_LIKELY(x)		__builtin_expect((x),1)
#define PTNK_UNLIKELY(x)	__builtin_expect((x),0)
#else
#define PTNK_LIKELY(x)		(x)
#define PTNK_UNLIKELY(x)	(x)
#endif

#define PTNK_STRINGIFY(x) PTNK_STRINGIFY2(x)
#define PTNK_STRINGIFY2(x) #x

#define PTNK_BSWAP32(x) __builtin_bswap32(x)
#define PTNK_BSWAP64(x) __builtin_bswap64(x)

#if defined(__GNUC__)
#define PTNK_MEMBARRIER_COMPILER asm volatile("": : :"memory");
#define PTNK_MEMBARRIER_HW __sync_synchronize()
#define PTNK_MEMBARRIER_HW_STORE asm volatile("sfence": : :"memory")
#define PTNK_MEMBARRIER_HW_LOAD asm volatile("lfence": : :"memory")
#define PTNK_CAS(ptr, from, to) __sync_bool_compare_and_swap((ptr), (from), (to))
#elif defined(_MSC_VER)
#define PTNK_MEMBARRIER_COMPILER _ReadWriteBarrier()
#define PTNK_MEMBARRIER_HW MemoryBarrier()
#define PTNK_MEMBARRIER_HW_STORE MemoryBarrier()
#define PTNK_MEMBARRIER_HW_LOAD MemoryBarrier()
#endif

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* defined in buffer.cpp */

/*! equivalent to libc memcmp but faster */
int ptnk_memcmp(const void* a, const void* b, size_t s);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#ifdef __cplusplus

#include <utility>
#include <algorithm>
#include <vector>
#include <memory>
#include <tuple>

namespace ptnk
{

using std::pair;
using std::make_pair;

using std::unique_ptr;
using std::shared_ptr;
using std::tie;

struct noncopyable
{
	noncopyable() = default;
	noncopyable(const noncopyable&) = delete;
	noncopyable& operator=(const noncopyable&) = delete;
};

// === misc template utils ===

template<typename T>
class ptr_match
{
	T* m_p;

public:
	ptr_match(T* p)
	:	m_p(p)
	{ /* NOP */ }

	bool operator()(const T& o) const
	{
		return &o == m_p;
	}
};

template<typename T>
inline
bool
vec_include_p(const std::vector<T>& v, const T& x)
{
	return std::find(v.begin(), v.end(), x) != v.end();
}

//! similar to std::lower_bound but operates by index
template<typename COMP>
inline
int
idx_lower_bound(int b, int e, COMP comp)
{
	int d = e - b, d2, m;
	while(d > 0)
	{
		d2 = d >> 1;
		m = b + d2;
		if(comp(m) < 0)
		{
			b = m+1;
			d -= d2+1;
		}
		else
		{
			d = d2;	
		}
	}
	return b;
}

template<typename COMP>
inline
int
idx_upper_bound(int b, int e, COMP comp)
{
	int d = e - b, d2, m;
	while(d > 0)
	{
		d2 = d >> 1;
		m = b + d2;
		if(comp(m) > 0)
		{
			d = d2;	
		}
		else
		{
			b = m+1;
			d -= d2+1;
		}
	}
	return b;
}

} // end of namespace ptnk

#endif /* __cplusplus */

#endif /* _ptnk_common_h_ */
