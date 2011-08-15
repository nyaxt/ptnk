#!/usr/sbin/dtrace -s

:::btree-get-start
{
	/* printf("dtrace get_start!!!"); */
	@func[probefunc] = count();
}

:::btree-put-start
{
	/* printf("dtrace put_start!!!"); */
	@func[probefunc] = count();
}
