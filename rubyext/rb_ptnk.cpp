#include <ruby.h>
#include <ruby/encoding.h>

// for htonl
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ptnk.h>

using namespace ptnk;

extern "C"
{

void Init_ptnk();

VALUE RBM_ptnk = Qnil;
VALUE RBK_db = Qnil;
VALUE RBK_dbtx = Qnil;

inline
BufferCRef
val2cref(VALUE val, VALUE* tmp)
{
	if(PTNK_UNLIKELY(NIL_P(val)))
	{
		return BufferCRef::NULL_VAL;	
	}
	else if(FIXNUM_P(val))
	{
		static unsigned long n = htonl(FIX2ULONG(val));
		return BufferCRef(&n, sizeof(unsigned long));
	}
	else
	{
		*tmp = StringValue(val);	
		return BufferCRef(RSTRING_PTR(*tmp), RSTRING_LEN(*tmp));
	}
}

struct rb_ptnk_db
{
	ptnk::DB* impl;
};

void
rb_ptnk_db_free(rb_ptnk_db* db)
{
	if(db->impl)
	{
		delete db->impl;
	}
}

VALUE
db_new(int argc, VALUE* argv, VALUE klass)
{
	VALUE ret;

	rb_ptnk_db* db;
	ret = Data_Make_Struct(klass, rb_ptnk_db, 0, rb_ptnk_db_free, db);

	const char* filename = "";
	ptnk_opts_t opts = ODEFAULT;
	int mode = 0644;

	switch(argc)
	{
	case 3:
		mode = FIX2INT(argv[2]);
		/* FALL THROUGH */
	
	case 2:
		opts = FIX2INT(argv[1]);
		/* FALL THROUGH */

	case 1:
		filename = StringValueCStr(argv[0]);
	}

	db->impl = new ptnk::DB(filename, opts, mode);

	return ret;
}
#define GET_DB_WRAP \
	rb_ptnk_db* db; \
	Data_Get_Struct(self, rb_ptnk_db, db);

typedef VALUE (*ruby_method_t)(...);

VALUE
db_get(VALUE self, VALUE key)
{
	GET_DB_WRAP;

	VALUE ret = rb_str_buf_new(2048);

	VALUE tmpKey;
	ssize_t sz = db->impl->get(val2cref(key, &tmpKey), BufferRef(RSTRING(ret)->as.heap.ptr, 2048));
	if(sz >= 0)
	{
		RSTRING(ret)->as.heap.len = sz;
		return ret;
	}
	else if(sz == BufferCRef::NULL_TAG)
	{
		return Qnil;	
	}
	else
	{
		// value not found
		return Qfalse;	
	}
}

VALUE
db_put(int argc, VALUE* argv, VALUE self)
{
	if(PTNK_UNLIKELY(argc < 2))
	{
		rb_raise(rb_eArgError, "only 1 arg given");	
	}

	GET_DB_WRAP;

	put_mode_t pm = PUT_UPDATE;
	if(argc > 3)
	{
		pm = (put_mode_t)FIX2INT(argv[2]);
	}

	VALUE tmpKey, tmpVal;
	db->impl->put(val2cref(argv[0], &tmpKey), val2cref(argv[1], &tmpVal), pm);
	
	return argv[1];
}

struct rb_ptnk_dbtx
{
	ptnk::DB::Tx* impl;
};

void
rb_ptnk_dbtx_free(rb_ptnk_dbtx* dbtx)
{
	if(dbtx->impl)
	{
		delete dbtx->impl;
	}
}

VALUE
dbtx_new(VALUE klass, VALUE vdb)
{
	VALUE ret;

	if(! rb_obj_is_kind_of(vdb, RBK_db))
	{
		rb_raise(rb_eArgError, "non Ptnk::DB passed to Ptnk::DB::Tx.new");	
	}
	rb_ptnk_db* db;
	Data_Get_Struct(vdb, rb_ptnk_db, db);

	rb_ptnk_dbtx* dbtx;
	ret = Data_Make_Struct(klass, rb_ptnk_dbtx, 0, rb_ptnk_dbtx_free, dbtx);

	dbtx->impl = db->impl->newTransaction();

	return ret;
}
#define GET_DBTX_WRAP \
	rb_ptnk_dbtx* dbtx; \
	Data_Get_Struct(self, rb_ptnk_dbtx, dbtx); \
	do { if(! dbtx->impl) rb_raise(rb_eRuntimeError, "tx already committed/aborted"); } while(0)

VALUE
dbtx_table_get_names(VALUE self)
{
	GET_DBTX_WRAP;

	VALUE ret, vname;
	ret = rb_ary_new();

	for(int i = 0;; ++ i)
	{
		Buffer name;
		dbtx->impl->tableGetName(i, &name);

		if(! name.isValid()) break;

		vname = rb_str_new(name.get(), name.valsize());

		rb_ary_push(ret, vname);
	}

	return ret;
}

VALUE
dbtx_try_commit(int argc, VALUE* argv, VALUE self)
{
	GET_DBTX_WRAP;

	// parse args
	bool bCommit = true;
	if(argc > 2) rb_raise(rb_eArgError, "too many args");
	if(argc == 1)
	{
		bCommit = RTEST(argv);
	}

	// try commit
	if(bCommit)
	{
		dbtx->impl->tryCommit();
	}
	delete dbtx->impl;
	dbtx->impl = NULL;
}

VALUE
dbtx_abort(VALUE self)
{
	GET_DBTX_WRAP;

	delete dbtx->impl;
	dbtx->impl = NULL;
}

void
Init_ptnk()
{
	RBM_ptnk = rb_define_module("Ptnk");
	RBK_db = rb_define_class_under(RBM_ptnk, "DB", rb_cObject);
	
	rb_define_singleton_method(RBK_db, "new", (ruby_method_t)db_new, -1);
	rb_define_method(RBK_db, "get", (ruby_method_t)db_get, 1);
	rb_define_method(RBK_db, "put", (ruby_method_t)db_put, -1);

	RBK_dbtx = rb_define_class_under(RBK_db, "Tx", rb_cObject);
	rb_define_singleton_method(RBK_dbtx, "new", (ruby_method_t)dbtx_new, 1);
	rb_define_method(RBK_dbtx, "table_get_names", (ruby_method_t)dbtx_table_get_names, 0);
	rb_define_method(RBK_dbtx, "try_commit", (ruby_method_t)dbtx_try_commit, -1);
	rb_define_method(RBK_dbtx, "abort", (ruby_method_t)dbtx_abort, 0);
}

} // extern "C"
