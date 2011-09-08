CXX=g++
HGNUM=$(shell hg id -n)
CXXFLAGS=-I/opt/local/include -I../build -Wall -DHGNUM="\"$(HGNUM)\"" -ggdb -O4
LDFLAGS=-L/opt/local/lib -L/usr/local/lib -lpthread -lgtest -lrt

TARGETS=kc_bench bdb_bench haildb_bench luxio_bench leveldb_bench

all: $(TARGETS)

.PHONY: clean all

bdb_bench: bdb_bench.cpp ../bench.h ../bench_tmpl.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -ldb -ldb_cxx -o $@ $<

kc_bench: kc_bench.cpp ../bench.h ../bench_tmpl.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -lkyotocabinet -o $@ $<

haildb_bench: haildb_bench.cpp ../bench.h ../bench_tmpl.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -lhaildb -o $@ $<

luxio_bench: luxio_bench.cpp ../bench.h ../bench_tmpl.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -lluxio -o $@ $<

leveldb_bench: leveldb_bench.cpp ../bench.h ../bench_tmpl.h
	$(CXX) -L/home/kouhei/work/leveldb -I/home/kouhei/work/leveldb/include $(CXXFLAGS) $(LDFLAGS) -o $@ $< -lleveldb

clean:
	rm $(TARGETS)
