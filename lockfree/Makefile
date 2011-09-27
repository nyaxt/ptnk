run: stm_test
	./stm_test

stm_test: draft.cpp
	g++ -g -std=c++0x -lrt -lgtest_main -lgtest -o $@ $<

clean:
	rm stm_test

.PHONY: run clean
