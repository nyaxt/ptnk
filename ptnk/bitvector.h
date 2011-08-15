#ifndef _ptnk_bitvector_h_
#define _ptnk_bitvector_h_

namespace ptnk
{

//! bit vector manipulator class
/*!
 *	the actual bits marked will be little different as treated byte-wise
 */
class BitVector
{
public:
	typedef unsigned long long manip_t;
	enum { MANIP_BITSZ = sizeof(manip_t)*8 };

	BitVector(void* start, size_t bitlength)
	: m_start(reinterpret_cast<manip_t*>(start)), m_bitlength(bitlength)
	{ /* NOP */ }

	void set(size_t i)
	{
		m_start[i / MANIP_BITSZ] |= ((manip_t)1) << (i % MANIP_BITSZ);
	}

	void clr(size_t i)
	{
		m_start[i / MANIP_BITSZ] &= ~(((manip_t)1) << (i % MANIP_BITSZ));
	}

	int get(size_t i) const
	{
		return (m_start[i / MANIP_BITSZ] >> (i % MANIP_BITSZ)) & (manip_t)1;	
	}

	static size_t popcnt_single(manip_t bv)
	{
		// FIXME: what if no builtin
		return __builtin_popcountll(bv);
	}

	size_t popcnt_to(size_t len) const
	{
		size_t ret = 0;

		unsigned int iE = len / MANIP_BITSZ;
		for(unsigned int i = 0; i < iE; ++ i)
		{
			ret += popcnt_single(m_start[i]);
		}
		unsigned int left = len % MANIP_BITSZ;
		if(left)
		{
			ret += popcnt_single(m_start[iE] & (((manip_t)~0) >> (MANIP_BITSZ - left)));
		}

		return ret;
	}

	size_t popcnt() const
	{
		return popcnt_to(m_bitlength);
	}

private:
	manip_t* m_start;
	size_t m_bitlength;
};

} // end of namespace ptnk

#endif // _ptnk_bitvector_h_
