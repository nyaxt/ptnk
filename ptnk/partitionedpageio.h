#ifndef _ptnk_partitionedpageio_h_
#define _ptnk_partitionedpageio_h_

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread.hpp>

#include "pageio.h"

namespace ptnk
{

class PartitionedPageIO : public PageIO
{
public:
	//! C-tor
	/*!
	 *	@param dbprefix
	 *		ex. "/db/file/path/dbname"
	 */
	PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode = 0644);

	~PartitionedPageIO();

	page_id_t alignCompactionThreshold(page_id_t threshold) const;

	// ====== implements PageIO interface ======

	virtual pair<Page, page_id_t> newPage();

	virtual Page readPage(page_id_t pgid);
	virtual Page modifyPage(const Page& page, mod_info_t* mod);
	virtual void sync(page_id_t pgid);
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	virtual page_id_t getLastPgId() const;
	virtual local_pgid_t getPartLastLocalPgId(part_id_t ptid) const;

	virtual bool needInit() const;

	virtual void newPart(bool bForce = false);
	virtual void discardOldPages(page_id_t threshold);

	virtual void dumpStat() const;

	// ====== inspection funcs for test ======
	
	size_t numPartitions_() const
	{
		return m_parts.size();	
	}

private:
	class Partition
	{
	public:
		Partition(part_id_t partid, const std::string& filepath, ptnk_opts_t optsPIO, int mode)
		:	m_partid(partid), m_filepath(filepath), m_optsPIO(optsPIO), m_mode(mode)
		{
			/* NOP */
		}

		part_id_t partid() const
		{
			return m_partid;	
		}

		const std::string& filepath() const
		{
			return m_filepath;	
		}

		PageIO* handler()
		{
			if(! m_parthandler.get())
			{
				// perform delayed instantiation of handler
				instantiateHandler();	
			}
			return m_parthandler.get();	
		}

		const PageIO* handler() const
		{
			return const_cast<Partition*>(this)->handler();
		}

		int numPages() const
		{
			return handler()->getLastPgId() - handler()->getFirstPgId() + 1;
		}

		void dumpStat() const;
		
	private:
		//! instantiate PageIO to m_parthandler
		void instantiateHandler();

		//! partition id
		part_id_t m_partid;

		//! path of file responsible for this partition
		std::string m_filepath;

		//! opts for instantiating handler PageIO
		ptnk_opts_t m_optsPIO;

		//! file mode for creating new file for this partition
		int m_mode;
		
		//! PageIO responsible for the partition
		std::auto_ptr<PageIO> m_parthandler;
	};
	typedef boost::ptr_vector<Partition> VPPartition;

	//! scan directory for partitioned db files and populate m_parts
	void scanFiles();

	//! add new partition
	Partition* addNewPartition();

	//! discard partition and delete its file
	void discardPartition(Partition* part);

	//! find partition from part id
	Partition* part(part_id_t partid);

	const Partition* part(part_id_t partid) const
	{
		return const_cast<PartitionedPageIO*>(this)->part(partid);	
	}

	//! db prefix str. see C-tor param
	std::string m_dbprefix;

	//! db file creat(2) perm
	int m_mode;	

	//! opts passed to partition handler PageIOs
	ptnk_opts_t m_opts;

	bool m_needInit;

	VPPartition m_parts;

	//! active partition where new pages are created
	/*!
	 *	all other partitions are read-only
	 */
	Partition* m_active;

	//! first loaded partition id
	part_id_t m_partidFirst;

	//! last partition id
	part_id_t m_partidLast;
};

} // end of namespace ptnk

#endif // _ptnk_partitionedpageio_h_
