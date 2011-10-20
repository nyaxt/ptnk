#ifndef _ptnk_tpio2_h_
#define _ptnk_tpio2_h_

#include "pageio.h"
#include "stm.h"

// Transactional PageIO impl. using stm.h

namespace ptnk
{

struct TPIOStat
{
	unsigned int numUniquePages;

	unsigned int nRead;
	unsigned int nReadOvr;
	unsigned int nReadOvrLocal;

	unsigned int nModifyPage;
	unsigned int nNewOvr;

	unsigned int nSync;

	unsigned int nNotifyOldLink;

	unsigned int nCommitFail;

	TPIOStat() :
		nRead(0),
		nReadOvr(0),
		nReadOvrLocal(0),
		nModifyPage(0),
		nNewOvr(0),
		nSync(0),
		nNotifyOldLink(0),
		nCommitFail(0)
	{ /* NOP */ }
	void dump() const;
};

class TPIO2;

class TPIO2TxSession : public PageIO
{
public:
	~TPIO2TxSession();

	bool tryCommit();

	// ====== implements PageIO interface ======
	pair<Page, page_id_t> newPage();

	Page readPage(page_id_t page); 
	Page modifyPage(const Page& page, mod_info_t* mod);
	void discardPage(page_id_t pgid, mod_info_t* mod);

	void sync(page_id_t pgid);

	page_id_t getLastPgId() const;

	void notifyPageWOldLink(page_id_t id, page_id_t idDep = PGID_INVALID);
	page_id_t updateLink(page_id_t idOld);

protected:
	friend class TPIO2; // give access to c-tor
	TPIO2TxSession(TPIO2* tpio, unique_ptr<LocalOvr>&& lovr);

	TPIO2* m_tpio;
	PageIO* getBackend();

	unique_ptr<LocalOvr> m_lovr;

	TPIOStat m_stat;
};

class TPIO2
{
public:
	TPIO2(boost::shared_ptr<PageIO> backend);

	~TPIO2();

	TPIO2TxSession* newTransaction();
	bool tryCommit(TPIO2TxSession* tx);

	PageIO* getBackend()
	{
		return m_backend.get();	
	}

private:
	boost::shared_ptr<PageIO> m_backend;

	ActiveOvr* m_aovr;
};

inline
bool
TPIO2TxSession::tryCommit()
{
	return m_tpio->tryCommit(this);	
}

inline
PageIO*
TPIO2TxSession::getBackend()
{
	return m_tpio->getBackend();
}

} // end of namespace ptnk

#endif // _ptnk_tpio2_h_
