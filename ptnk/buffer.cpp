#include "buffer.h"

#include <boost/lexical_cast.hpp>

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
		std::string ret;
		if(size() == 1)
		{
			ret.append(boost::lexical_cast<std::string>((int)*(const uint8_t*)get()));
			ret.append("I: ");
		} 
		else if(size() == 4)
		{
			uint32_t i = __builtin_bswap32(*(const uint32_t*)get());
			ret.append(boost::lexical_cast<std::string>(i));
			ret.append("I: ");
		}

		for(ssize_t i = 0; i < size(); ++ i)
		{
			char c = *(get() + i);
			if(isprint(c))
			{
				ret.push_back(c);
			}
			else
			{
				// ret.push_back('?');
			}
		}

		return ret;
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

		for(ssize_t i = 0; i < size(); ++ i)
		{
			out << (boost::format("%02x") % (int)(uint8_t)get()[i]);
			if(i % 4 == 3)
			{
				out << ' ';	
			}
		}

		return out.str();
	}
}

} // end of namespace ptnk
