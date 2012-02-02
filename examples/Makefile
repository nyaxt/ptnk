CXX=g++
CXXFLAGS := $(CXXFLAGS) -O2
LDFLAGS := $(LDFLAGS) -lptnk

run_demo: cxx_demo
	./cxx_demo hoge

cxx_demo: cxx_demo.cpp
	$(CXX) $(CXXFLAGS) $< $(LDFLAGS) -o $@

clean:
	@rm cxx_demo hoge*.ptnk

.PHONY: run_demo cxx_demo
