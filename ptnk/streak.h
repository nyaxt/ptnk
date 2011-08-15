#ifndef _ptnk_streak_h_
#define _ptnk_streak_h_

#include "pageio.h"

namespace ptnk
{

class OverflowedStreakPage : public Page
{
public:
	enum
	{
		TYPE = PT_OVFLSTREAK,
	};

	OverflowedStreakPage()
	{ /* NOP */ }

	explicit OverflowedStreakPage(const Page& pg, bool force = false)
	{
		if(! force) { PTNK_ASSERT(pg.pageType() == PT_OVFLSTREAK); }
		*reinterpret_cast<Page*>(this) = pg;
	}

	void init(page_id_t id)
	{
		initHdr(id, PT_OVFLSTREAK);	
		size() = 0;
	}

	BufferCRef write(BufferCRef buf)
	{
		PTNK_ASSERT(! buf.empty());
		size_t left = PTNK_BODY_SIZE + PTNK_STREAK_SIZE - sizeof(size_t) - size();
		size_t wlen = std::min(static_cast<size_t>(buf.size()), left);
		::memcpy(data() + size(), buf.popFront(wlen), wlen);
		size() += wlen;

		return buf;
	}

	BufferCRef read()
	{
		return BufferCRef(data(), size());	
	}

private:
	size_t& size()
	{
		return *reinterpret_cast<size_t*>(rawbody());	
	}

	char* data()
	{
		return rawbody() + sizeof(size_t);	
	}
};

template<typename it_t>
class StreakIO
{
public:
	StreakIO(const it_t& it, const it_t& itE, PageIO* pio)
	:	m_it(it), m_itE(itE), m_offset(0), m_pgidOvfl(PGID_INVALID), m_pio(pio)
	{ /* NOP */ }

	void write(BufferCRef buf)
	{
		while(! buf.empty() && m_it != m_itE)
		{
			Page pg(m_pio->readPage(*m_it));

			size_t left = Page::STREAK_SIZE - m_offset;
			size_t wlen = std::min(static_cast<size_t>(buf.size()), left);
			// printf("streak %p + offset %d\n", pg.streak(), m_offset);
			::memcpy(pg.streak() + m_offset, buf.popFront(wlen), wlen);
			left -= wlen;

			// m_pio->sync(pg);

			if(left > 0)
			{
				m_offset += wlen;
			}
			else
			{
				m_offset = 0;
				++m_it;
			}
		}
		for(; ! buf.empty(); m_pgidOvfl = PGID_INVALID)
		{
			OverflowedStreakPage ospg;
			
			if(m_pgidOvfl == PGID_INVALID)
			{
				ospg = m_pio->newInitPage<OverflowedStreakPage>();
				m_pgidOvfl = ospg.pageId();
			}
			else
			{
				ospg = OverflowedStreakPage(m_pio->readPage(m_pgidOvfl));
			}

			buf = ospg.write(buf);
			m_pio->sync(ospg);
		}
	}

private:
	it_t m_it, m_itE;

	size_t m_offset;

	page_id_t m_pgidOvfl;

	PageIO* m_pio;
};

} // end of namespace ptnk

#endif // _ptnk_streak_h_
