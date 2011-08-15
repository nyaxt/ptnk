.SUFFIXES: .gv .png
.PHONY: all

gvfiles=$(wildcard *.gv)
pngfiles=$(gvfiles:.gv=.png)

all: $(pngfiles)
	@echo $(pngfiles)

.gv.png:
	dot -Tpng -o $@ $<
