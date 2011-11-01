#include "db.h"

#include <stdio.h>

#include <boost/tuple/tuple.hpp>

#include "pageiomem.h"
#include "partitionedpageio.h"
#include "btree.h"
#include "tpio.h"
#include "tpio2.h"
#include "overview.h"
#include "helperthr.h"
#include "sysutils.h"

namespace ptnk
{

DB::DB(const char* filename, ptnk_opts_t opts, int mode)
{
	if(opts & OHELPERTHREAD)
	{
		m_helper.reset(new Helper);	
	}

	if(filename && *filename != '\0' && (opts & OPARTITIONED))
	{
		PartitionedPageIO* ppio;
		m_pio.reset((ppio = new PartitionedPageIO(filename, opts, mode)));

		ppio->attachHelper(m_helper.get());
	}
	else
	{
		m_pio.reset(new PageIOMem(filename, opts, mode));
	}

	initCommon();
}

DB::DB(const shared_ptr<PageIO>& pio)
{
	m_pio = pio;
	initCommon();
}

void
DB::drop(const char* filename)
{
	if(!filename || *filename == '\0') return;
	
	if(file_exists(filename))
	{
		PTNK_ASSURE_SYSCALL(::unlink(filename));
	}
	else
	{
		// check for partitioned db files
		PartitionedPageIO::drop(filename);
	}
}

void
DB::initCommon()
{
	m_tpio.reset(new TPIO2(m_pio));

	if(m_pio->needInit())
	{
		boost::scoped_ptr<Tx> tx(newTransaction());

		OverviewPage pgOvv(tx->pio()->newInitPage<OverviewPage>());
		tx->pio()->setPgidStartPage(pgOvv.pageId());

		tx->tableCreate(cstr2ref("default"));
		
		PTNK_CHECK_CMNT(tx->tryCommit(), "init tx should not fail!");
	}
}

DB::~DB()
{
	/* NOP */
}

ssize_t
DB::get(BufferCRef key, BufferRef value)
{
	Tx tx(this, m_tpio->newTransaction());
	return tx.get(key, value);
}

void
DB::put(BufferCRef key, BufferCRef value, put_mode_t mode)
{
	for(;;)
	{
		Tx tx(this, m_tpio->newTransaction());
		tx.put(key, value, mode);
		if(tx.tryCommit()) break;
	}
}

DB::Tx*
DB::newTransaction()
{
	Tx* tx = new Tx(this, m_tpio->newTransaction());
	return tx;
}

DB::Tx::Tx(DB* db, unique_ptr<TPIO2TxSession> pio)
:	m_bCommitted(false),
	m_db(db),
	m_pio(move(pio))
{
	/* NOP */
}

DB::Tx::~Tx()
{
	/* NOP */
}

void
DB::Tx::tableCreate(BufferCRef table)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	if(PGID_INVALID != pgOvv.getTableRoot(table))
	{
		PTNK_THROW_RUNTIME_ERR("table already exists");	
	}
	
	page_id_t pgidRoot = btree_init(m_pio.get());

	pgOvv.setTableRoot(table, pgidRoot, NULL, m_pio.get());
}

void
DB::Tx::tableDrop(BufferCRef table)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));

	pgOvv.dropTable(table, NULL, m_pio.get());
}

ssize_t
DB::Tx::tableGetName(int idx, BufferRef name)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	
	return bufcpy(name, pgOvv.getTableName(idx));
}

ssize_t
DB::Tx::get(BufferCRef table, BufferCRef key, BufferRef value)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	page_id_t pgidRoot = pgOvv.getTableRoot(table);
	if(pgidRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");

	return btree_get(pgidRoot, key, value, m_pio.get());
}

ssize_t
DB::Tx::get(TableOffCache* table, BufferCRef key, BufferRef value)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	page_id_t pgidRoot = pgOvv.getTableRoot(table);
	if(pgidRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");

	return btree_get(pgidRoot, key, value, m_pio.get());
}

ssize_t
DB::Tx::get(BufferCRef key, BufferRef value)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	return btree_get(pgOvv.getDefaultTableRoot(), key, value, m_pio.get());
}

void
DB::Tx::put(BufferCRef table, BufferCRef key, BufferCRef value, put_mode_t mode)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));

	page_id_t pgidOldRoot = pgOvv.getTableRoot(table);
	if(pgidOldRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");
	page_id_t pgidNewRoot = btree_put(pgidOldRoot, key, value, mode, pgOvv.pageOrigId(), m_pio.get());
	// m_pio->notifyPageWOldLink(pgOvv.pageOrigId()); // this can be safely omitted
	
	// handle root node update
	if(pgidNewRoot != pgidOldRoot)
	{
		pgOvv.setTableRoot(table, pgidNewRoot, NULL, m_pio.get());
	}
}

void
DB::Tx::put(TableOffCache* table, BufferCRef key, BufferCRef value, put_mode_t mode)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));

	page_id_t pgidOldRoot = pgOvv.getTableRoot(table);
	if(pgidOldRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");
	page_id_t pgidNewRoot = btree_put(pgidOldRoot, key, value, mode, pgOvv.pageOrigId(), m_pio.get());
	// m_pio->notifyPageWOldLink(pgOvv.pageOrigId()); // this can be safely omitted
	
	// handle root node update
	if(pgidNewRoot != pgidOldRoot)
	{
		pgOvv.setTableRoot(table, pgidNewRoot, NULL, m_pio.get());
	}
}

void
DB::Tx::put(BufferCRef key, BufferCRef value, put_mode_t mode)
{
	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));

	page_id_t pgidOldRoot = pgOvv.getDefaultTableRoot();
	page_id_t pgidNewRoot = btree_put(pgidOldRoot, key, value, mode, pgOvv.pageOrigId(), m_pio.get());
	// m_pio->notifyPageWOldLink(pgOvv.pageOrigId()); // this can be safely omitted
	
	// handle root node update
	if(pgidNewRoot != pgidOldRoot)
	{
		pgOvv.setDefaultTableRoot(pgidNewRoot, NULL, m_pio.get());
	}
}

struct DB::Tx::cursor_t
{
	page_id_t pgidRoot;
	btree_cursor_t* curBTree;
	Buffer tableid;
};

void
DB::Tx::curClose(cursor_t* cur)
{
	if(cur) delete cur;
}

inline
DB::Tx::cursor_t*
DB::Tx::curNew(BufferCRef table)
{
	unique_ptr<cursor_t> cur(new cursor_t);

	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	cur->pgidRoot = pgOvv.getTableRoot(table);
	if(cur->pgidRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");
	cur->curBTree = btree_cursor_new();
	cur->tableid = table;

	return cur.release();
}

inline
DB::Tx::cursor_t*
DB::Tx::curNew(TableOffCache* table)
{
	unique_ptr<cursor_t> cur(new cursor_t);

	OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
	cur->pgidRoot = pgOvv.getTableRoot(table);
	if(cur->pgidRoot == PGID_INVALID) PTNK_THROW_RUNTIME_ERR("table not found");
	cur->curBTree = btree_cursor_new();
	cur->tableid = table->getTableId();

	return cur.release();
}

DB::Tx::cursor_t*
DB::Tx::curFront(BufferCRef table)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_cursor_front(cur->curBTree, cur->pgidRoot, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

DB::Tx::cursor_t*
DB::Tx::curBack(BufferCRef table)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_cursor_back(cur->curBTree, cur->pgidRoot, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

DB::Tx::cursor_t*
DB::Tx::curQuery(BufferCRef table, const query_t& q)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_query(cur->curBTree, cur->pgidRoot, q, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

DB::Tx::cursor_t*
DB::Tx::curFront(TableOffCache* table)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_cursor_front(cur->curBTree, cur->pgidRoot, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

DB::Tx::cursor_t*
DB::Tx::curBack(TableOffCache* table)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_cursor_back(cur->curBTree, cur->pgidRoot, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

DB::Tx::cursor_t*
DB::Tx::curQuery(TableOffCache* table, const query_t& q)
{
	unique_ptr<cursor_t> cur(curNew(table));

	btree_query(cur->curBTree, cur->pgidRoot, q, m_pio.get());

	if(btree_cursor_valid(cur->curBTree))
	{
		return cur.release();
	}
	else
	{
		return NULL;	
	}
}

bool
DB::Tx::curNext(cursor_t* cur)
{
	return btree_cursor_next(cur->curBTree, m_pio.get());
}

bool
DB::Tx::curPrev(cursor_t* cur)
{
	return btree_cursor_prev(cur->curBTree, m_pio.get());
}

void
DB::Tx::curGet(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, cursor_t* cur)
{
	btree_cursor_get(key, szKey, value, szValue, cur->curBTree, m_pio.get());
}

void
DB::Tx::curPut(cursor_t* cur, BufferCRef value)
{
	page_id_t pgidOldRoot = btree_cursor_root(cur->curBTree);
	page_id_t pgidNewRoot = btree_cursor_put(cur->curBTree, value, m_pio.get());

	if(pgidNewRoot != pgidOldRoot)
	{
		OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
		PTNK_ASSERT(pgidOldRoot == pgOvv.getTableRoot(cur->tableid.rref()));
		
		pgOvv.setTableRoot(cur->tableid.rref(), pgidNewRoot, NULL, m_pio.get());
	}
}

bool
DB::Tx::curDelete(cursor_t* cur)
{
	page_id_t pgidOldRoot = btree_cursor_root(cur->curBTree);
	page_id_t pgidNewRoot;
	bool bNextExist;

	boost::tie(bNextExist, pgidNewRoot) = btree_cursor_del(cur->curBTree, m_pio.get());

	if(pgidNewRoot != pgidOldRoot)
	{
		OverviewPage pgOvv(m_pio->readPage(m_pio->pgidStartPage()));
		PTNK_ASSERT(pgidOldRoot == pgOvv.getTableRoot(cur->tableid.rref()));
		
		pgOvv.setTableRoot(cur->tableid.rref(), pgidNewRoot, NULL, m_pio.get());
	}

	return bNextExist;
}

bool
DB::Tx::tryCommit()
{
	PTNK_ASSERT(! m_bCommitted);

	if(m_pio->tryCommit())
	{
		m_bCommitted = true;
		m_db->rebase(false); // rebase if needed

		return true;
	}
	else
	{
		return false;	
	}
}

void
DB::Tx::dumpStat() const
{
	std::cout << *m_pio << std::endl;
}

void
DB::rebase(bool force)
{
	m_tpio->rebase(force);
}

void
DB::newPart(bool doRebase)
{
	if(doRebase)
	{
		rebase(/* force */ true);	
	}

	m_pio->newPart();
}

void
DB::compact()
{
	m_pio->newPart(false /* no force */);

	PartitionedPageIO* ppio = dynamic_cast<PartitionedPageIO*>(m_pio.get());
	if(! ppio)
	{
		std::cerr << "ptnk::DB::compact called on unsupported PageIO impl." << std::endl;
		return;	
	}

	std::cout << "* starting compaction." << std::endl;

	std::cout << "lastpgid: " << pgid2str(m_pio->getLastPgId()) << std::endl;
	std::cout << "numuniqpgs: " << m_tpio->stat().nUniquePages << std::endl;
	page_id_t threshold = m_pio->getLastPgId() - m_tpio->stat().nUniquePages * 3;
	std::cout << "orig threshold : " << pgid2str(threshold) << std::endl;
	threshold = ppio->alignCompactionThreshold(threshold);
	std::cout << "aligned threshold : " << pgid2str(threshold) << std::endl;

	if(threshold == PGID_INVALID)
	{
		std::cerr << "aligned threshold says that compaction is impossible" << std::endl;
		return;	
	}

	m_tpio->refreshOldPages(threshold);
	std::cout << "refreshOldPages done." << std::endl;
	
	m_pio->discardOldPages(threshold);
	std::cout << "discardOldPages done." << std::endl;
}

void
DB::dump() const
{
	unique_ptr<TPIO2TxSession> tx(m_tpio->newTransaction());
	OverviewPage(tx->readPage(tx->pgidStartPage())).dump(tx.get());
}

void
DB::dumpGraph(FILE* fp) const
{
	fprintf(fp, "digraph bptree {\n");
	unique_ptr<TPIO2TxSession> tx(m_tpio->newTransaction());
	OverviewPage(tx->readPage(tx->pgidStartPage())).dumpGraph(fp, tx.get());
	fprintf(fp, "}\n");
}

void
DB::dumpStat() const
{
	std::cout << "*** DB dump ***" << std::endl;
	std::cout << *m_tpio << std::endl;
}

void
DB::dumpAll()
{
	std::cout << "*** DB full dump ***" << std::endl;
	{
		unique_ptr<TPIO2TxSession> tx(m_tpio->newTransaction());
		std::cout << "* overview pgid : " << tx->pgidStartPage() << std::endl;
	}

	const part_id_t partidE = m_pio->getPartLastLocalPgId(m_pio->getLastPgId());
	for(part_id_t partid = PGID_PARTID(m_pio->getFirstPgId()); partid <= partidE; ++ partid)
	{
		const local_pgid_t pgidLE = m_pio->getPartLastLocalPgId(partid);
		page_id_t pgid = 0;
		for(local_pgid_t pgidL = 0; pgidL <= pgidLE; ++ pgidL, pgid = PGID_PARTLOCAL(partid, pgidL))
		{
			Page pg(m_pio->readPage(pgid));
			pg.dump(NULL); // no recursive dump //m_pio.get());
		}
	}
}

} // end of namespace ptnk
