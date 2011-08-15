#include <iostream>

// for htonl
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ptnk/db.h>

int
main(int argc, char* argv[])
{
	if(argc < 2)
	{
		printf("USAGE: %s [dbfile]\n", argv[0]);	
		return 1;
	}

	try
	{
		// 1. initialize database instance
		//   
		//   Database obj. _db_ is instantiated.
		//   As ODEFAULT = OWRITER | OCREATE | OAUTOSYNC | OPARTITIONED,
		//	 new db files are created if not exist.
		//	 _db_ obj is managed through RAII idiom. so all resource cleanup operations
		//	 (such as file close, memory free) are handled automatically at destructor.
		//
		const char* strFilename = argv[1];
		ptnk::DB db(strFilename, /* options = */ ptnk::ODEFAULT, /* creat(2) mode */ 0644);

		// 2. non-transactional APIs
		//
		//   "Put" and "get" APIs are provided for simple interactions with the DB.
		//   Arbitrary byte sequence can be used as key and value.
		//
		//   For more complex operations, use of transactional APIs (described in 3)
		//   are recommended.
		//

		//   db.put(key, value, put_mode) associates _value_ to _key_.
		//   put_mode specifies what to do when other record with the same key was found.
		//   PUT_UPDATE forces old value to be overwritten by new value.
		//
		//   cstr2ref is helper function for specifying null-terminated strings
		db.put(
			ptnk::cstr2ref("key"), ptnk::cstr2ref("value"),
			/* put_mode = */ ptnk::PUT_UPDATE
			);

		//   BufferCRef obj is used to specify arbitrary byte sequence for key and value.
		//   usage: BufferCRef(ptr, size)
		{
			char keyblob[] = {0x1, 0x3};
			char valueblob[] = {0x2, 0x3, 0x4};
			db.put(
				ptnk::BufferCRef(keyblob, sizeof(keyblob)),
				ptnk::BufferCRef(valueblob, sizeof(valueblob)),
				ptnk::PUT_UPDATE
				);
		}

		// NOTE: 
		//   When using int as key values, we encourage it to be stored using big-endian byte order.
		//	 In current implementation of ptnk, memcmp is used to define order of keys.
		{
			int key = 100;
			unsigned long tmp = htonl(key);
			ptnk::BufferCRef key_blob(&tmp, 4);
			db.put(key_blob, ptnk::cstr2ref("100"), ptnk::PUT_UPDATE);
		}

		//   stored value can be retrieved by db.get call.
		//   BufferRef obj is used to specify buffer to retrieved value.
		//   usage: BufferRef(ptr, max_allowed_size)
		{
			char result[255];
			size_t valsize = db.get(
				ptnk::cstr2ref("key"),
				ptnk::BufferRef(result, sizeof(result)-1)
				);
			result[valsize] = '\0'; // null terminate result string

			std::cout << "got value str: " << result << std::endl;
		}
		
		//   Above get made simple using Buffer helper obj.
		//   Buffer can be used to hold arbitrary byte seq.
		{
			ptnk::Buffer value;
			db.get(ptnk::cstr2ref("key"), &value);

			value.makeNullTerm(); // null terminate result string
			std::cout << "got value str: " << value.get() << std::endl;
		}

		// 3. Transactional APIs
		//
		//   Ptnk supports transactions. This can be used to:
		//   - atomically issue changes to db at once
		//   - speed up insertions of records by "put"-ting several records at once
		//     and reducing disc sync
		//   Multi-version concurrency control is used to implement transactions in ptnk.
		//   Each transactions operate on separate "snapshots" created when Tx object is instantiated.
		//
		//	 It is ensured that transactions will
		//	 - always see consistent view of the database (a.k.a. repeatable read)
		//	 - NOT see uncommitted records by other transactions
		//
		//   They are committed at the end when no conflicts are found.
		//   (NOTE: currently only conflicts detected are WRITE-WRITE conflicts)
		//
		{
			int b = 200;
			db.put(ptnk::cstr2ref("balance"), ptnk::BufferCRef(&b, sizeof(int)));
		}
		for(;;)
		{
			// create new transaction obj.
			std::auto_ptr<ptnk::DB::Tx> tx(db.newTransaction());

			ptnk::Buffer v;
			tx->get(ptnk::cstr2ref("balance"), &v);

			int b = *reinterpret_cast<int*>(v.get());
			int new_b = b - 100;

			tx->put(ptnk::cstr2ref("balance"), ptnk::BufferCRef(&new_b, sizeof(int)), ptnk::PUT_UPDATE);

			// try committing the transaction and exit loop if success
			if(tx->tryCommit()) break;
		}
	}
	catch(std::exception& e)
	{
		// Errors from ptnk C++ API are notified by C++ exceptions.
		// all exception objects inherit std::exception, and can be inspected via e.what()

		std::cerr << "caught exception: " << e.what() << std::endl;
	}

	return 0;
}
