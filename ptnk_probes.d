provider ptnk {
	probe btree__put__start();
	probe btree__put__end();

	probe btree__get__start();
	probe btree__get__end();

	probe btree__del__start();
	probe btree__del__end();
};
