#ifndef _ptnk_buffer_h_
#define _ptnk_buffer_h_
#include "common.h"

#include <stdio.h>
#include <iostream>
#include <string>
#include <memory>
#include <string.h>

#include "exceptions.h"

namespace ptnk
{

inline
int
ptnk_memcmp_inl(const void* a, const void* b, size_t s)
{
#ifdef PTNK_USE_CUSTOM_MEMCMP
	const uint64_t* ia = static_cast<const uint64_t*>(a);
	const uint64_t* ib = static_cast<const uint64_t*>(b);

	for(;s > 7; s -= 8)
	{
		int64_t diff = PTNK_BSWAP64(*ia++) - PTNK_BSWAP64(*ib++);

		if(diff > 0)
		{
			return 1;
		}
		else if(diff < 0)
		{
			return -1;	
		}
	}

	if(s == 0) return 0;
	
	uint8_t sh = (8 - s)*8;
	int64_t diff = (PTNK_BSWAP64(*ia << sh) - PTNK_BSWAP64(*ib << sh));
	if(diff > 0)
	{
		return 1;
	}
	else if(diff < 0)
	{
		return -1;	
	}
	else
	{
		return 0;
	}
#else
	return ::memcmp(a, b, s);
#endif
}


class BufferCRef;

class BufferRef
{
public:
	BufferRef()
	:	m_p(NULL), m_size(-1)
	{ /* NOP */ }

	BufferRef(void* p, ssize_t size)
	:	m_p(static_cast<char*>(p)), m_size(size)
	{ /* NOP */}

	char* get() const
	{
		return m_p;	
	}

	ssize_t size() const
	{
		return m_size;
	}

	bool isValid() const
	{
		return m_p != NULL && m_size >= 0;
	}

	bool empty() const
	{
		return m_p == NULL || m_size <= 0;
	}

	void reset()
	{
		m_p = NULL; m_size = -1;	
	}

private:
	char* m_p;
	ssize_t m_size;
};

inline
std::ostream&
operator<<(std::ostream& s, BufferRef o)
{
	s << "BufferRef<" << (void*)o.get() << "[" << o.size() << "]>";
	return s;
}

//! class representing read-only reference to a memory region
/*!
 *	This class is used for manipulating key and value blob.
 *
 *	There are three states of BufferCRef reference. Notice that <b>invalid</b> and <b>NULL</b> state are different. 
 *	<table>
 *	<tr><th>state desc.</th>	<th>isValid()</th>	<th>isNull()</th>	<th>empty()</th></tr>
 *	<tr><td>valid value</td>	<td>true</td>		<td>false</td>		<td>true if size == 0</td></tr>
 *	<tr><td>NULL value</td>		<td>true</td>		<td>true</td>		<td>true</td></tr>
 *	<tr><td>invalid value</td>	<td>false</td>		<td>false</td>		<td>true</td></tr>
 *	</table>
 *
 *  The <b>invalid</b> state is the initial state of BufferCRef when created by default c-tor. and is typically used to represent that no entry for the given key exist in the database.
 *  The <b>NULL</b> state is used to implement NULL fields as in RDBMS. This means that the entry for the given key exists, but no value is assigned.
 */
class BufferCRef
{
public:
	//! initial state represent "invalid" value
	BufferCRef(bool init = true)
	{
		if(init)
		{
			m_p = NULL;
			m_size = -1;
		}
	}

	BufferCRef(const void* p, ssize_t size)
	:	m_p(static_cast<const char*>(p)), m_size(size)
	{ /* NOP */}

	BufferCRef(const BufferRef& o)
	:	m_p(o.get()), m_size(o.size())
	{ /* NOP */}

	const char* get() const
	{
		return m_p;
	}

	enum
	{
		INVALID_TAG = -1,
		NULL_TAG = -2,	
	};

	ssize_t size() const
	{
		return m_size;
	}

	ssize_t packedsize() const
	{
		return isNull() ? 0 : m_size;	
	}

	//! returns true if this represent some meaningful data (including NULL)
	bool isValid() const
	{
		return (m_p != NULL && m_size >= 0) || isNull();
	}

	//! returns true if this represent NULL value
	bool isNull() const
	{
		return m_size == NULL_TAG;	
	}

	//! returns true if no more data can be retrieved by get()
	/*!
	 *	You should check that this is false before popFront() / popFrontTo().
	 */
	bool empty() const
	{
		return m_p == NULL || m_size <= 0;
	}

	void reset()
	{
		m_p = NULL; m_size = -1;	
	}

	const char* popFront(size_t l)
	{
		PTNK_ASSERT(m_size >= (ssize_t)l);

		const char* ret = m_p;

		m_size -= l;
		m_p += l;

		return ret;
	}

	char* popFrontTo(void* to, size_t len)
	{
		::memcpy(to, popFront(len), len);
		return reinterpret_cast<char*>(to) + len;
	}

	void popFrontTo(BufferRef tgt)
	{
		::memcpy(tgt.get(), popFront(tgt.size()), tgt.size());
	}

	std::string inspect() const;

	std::string hexdump() const;

	//! buffer val representing NULL
	static const BufferCRef NULL_VAL;

	//! buffer val representing invalid state
	static const BufferCRef INVALID_VAL;

private:
	const char* m_p;
	ssize_t m_size;
};

inline
std::ostream&
operator<<(std::ostream& s, BufferCRef o)
{
	s << "<" << (void*)o.get() << "[" << o.size() << "] = " << o.inspect() << ">";
	return s;
}

inline
BufferCRef
cstr2ref(const char* str)
{
	return BufferCRef(str, ::strlen(str));
}

inline
size_t bufcpy(BufferRef dest, BufferCRef src)
{
	if(PTNK_UNLIKELY(src.empty())) return src.size();

	size_t size = dest.size() > src.size() ? src.size() : dest.size();
	::memcpy(dest.get(), src.get(), size);
	return size;
}

inline
int bufcmp(BufferCRef a, BufferCRef b)
{
	if(a.size() != b.size())
	{
		return a.size() - b.size();	
	}

	if(a.empty() /* && b.empty() */) return 0;

#if PTNK_DEBUG_VERIFY_MEMCMP
	int x = ptnk_memcmp_inl(a.get(), b.get(), a.size());
	int y = ::memcmp(a.get(), b.get(), a.size());

	if((y == 0 && x != 0) || (x < 0 && y > 0) || (x > 0 && y < 0))
	{
		*(char*)0x0	= 1; //trap
	}
	return y;
#else
	return ptnk_memcmp_inl(a.get(), b.get(), a.size());
#endif
}

inline
bool bufeq(BufferCRef a, BufferCRef b)
{
	// TODO: optimize here?
	return 0 == bufcmp(a, b);
}

class Buffer : noncopyable
{
public:
	enum
	{
		SZ_RESV = 256,
		NULL_TAG = -2,
	};

	Buffer(size_t size = SZ_RESV)
	:	m_valsize(-1)
	{
		if(size > sizeof(m_resv))
		{
			m_p = static_cast<char*>(::malloc(size));
			m_size = size;
		}
		else
		{
			m_p = m_resv;
			m_size = sizeof(m_resv);
		}
	}

	~Buffer()
	{
		if(m_p && m_p != m_resv)
		{
			::free(m_p);
		}
	}

	BufferRef wref()
	{
		return BufferRef(m_p, m_size);
	}

	const Buffer& operator=(BufferCRef ref)
	{
		PTNK_ASSERT(ref.isValid());
		if(ref.isNull())
		{
			setValsize(NULL_TAG);	
		}
		else
		{
			setValsize(bufcpy(wref(), ref));
		}

		return *this;
	}

	const Buffer& operator=(const Buffer& o)
	{
		return *this = o.rref();
	}

	BufferCRef rref() const
	{
		return BufferCRef(m_p, m_valsize);	
	}

	char* get()
	{
		return m_p;	
	}

	const char* get() const
	{
		return m_p;	
	}

	//! value size
	ssize_t valsize() const
	{
		return m_valsize;
	}

	bool isNull() const
	{
		return m_valsize == NULL_TAG;	
	}

	bool isValid() const
	{
		return (m_valsize >= 0 && m_p) || (m_valsize == NULL_TAG);
	}

	ssize_t* pvalsize()
	{
		return &m_valsize;	
	}

	void setValsize(ssize_t size)
	{
		m_valsize = size;	
	}

	//! reserved buffer size
	size_t ressize() const
	{
		return m_size;
	}

	void resize(size_t newsize)
	{
		PTNK_ASSERT(newsize >= m_size);	
		if(m_p == m_resv)
		{
			m_p = static_cast<char*>(::malloc(newsize));
			::memcpy(m_p, m_resv, m_size);
		}
		else
		{
			m_p = static_cast<char*>(::realloc(m_p, newsize));
		}
		m_size = newsize;
	}

	BufferCRef append(const BufferCRef& ref)
	{
		if(ref.empty()) return ref;

		if(m_valsize < 0) m_valsize = 0;

		if(static_cast<size_t>(m_valsize + ref.size()) > m_size)
		{
			resize(std::max(ressize() * 2, static_cast<size_t>(m_valsize + ref.size())));
		}

		char* off = m_p + m_valsize;
		::memcpy(off, ref.get(), ref.size());
		m_valsize += ref.size();

		return BufferCRef(off, ref.size());
	}

	void makeNullTerm()
	{
		if(m_valsize < 0) return;

		if(PTNK_UNLIKELY(static_cast<size_t>(m_valsize + 1) > m_size))
		{
			resize(std::max(ressize() * 2, static_cast<size_t>(m_valsize + 1)));
		}

		m_p[m_valsize] = '\0';
		// ++ m_valsize
	}

	void reset()
	{
		m_valsize = -1;
	}

private:
	char* m_p;
	char m_resv[SZ_RESV];
	ssize_t m_valsize;
	size_t m_size;
};

inline
std::ostream&
operator<<(std::ostream& s, const Buffer& o)
{
	s << "BUF[" << o.ressize() << "]: " << o.rref();
	return s;
}

} // end of namespace ptnk

#endif // _ptnk_buffer_h_
