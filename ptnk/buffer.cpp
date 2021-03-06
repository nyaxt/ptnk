#include "buffer.h"

#include <sstream>
#include <iomanip>

int
ptnk_memcmp(const void* a, const void* b, size_t s)
{
	return ptnk::ptnk_memcmp_inl(a, b, s);
}

namespace ptnk
{

const BufferCRef BufferCRef::NULL_VAL = BufferCRef(0, BufferCRef::NULL_TAG);
const BufferCRef BufferCRef::INVALID_VAL = BufferCRef(0, BufferCRef::INVALID_TAG);

std::string
BufferCRef::inspect() const
{
	if(! isValid())
	{
		return std::string("INVALID");	
	}
	else if(isNull())
	{
		return std::string("NULL");
	}
	else 
	{
		std::stringstream ret;
		if(size() == 1)
		{
			ret << static_cast<int>(*reinterpret_cast<const uint8_t*>(get())) << "I: ";
		} 
		else if(size() == 4)
		{
			uint32_t i = PTNK_BSWAP32(*(const uint32_t*)get());
			ret << i << "I: ";
		}

		for(ssize_t i = 0; i < size(); ++ i)
		{
			char c = *(get() + i);
			if(isprint(c))
			{
				ret << c;
			}
			else
			{
				// ret.push_back('?');
			}
		}

		return ret.str();
	}
}

std::string
BufferCRef::hexdump() const
{
	if(! isValid())
	{
		return std::string("INVALID");	
	}
	else if(isNull())
	{
		return std::string("NULL");
	}
	else 
	{
		std::stringstream out;
		out << std::setfill('0'); // this interface really sucks!!!

		for(ssize_t i = 0; i < size(); ++ i)
		{
			out << std::setw(2) << std::hex << static_cast<unsigned int>(get()[i]);
			if(i % 4 == 3)
			{
				out << ' ';	
			}
		}

		return out.str();
	}
}

} // end of namespace ptnk
