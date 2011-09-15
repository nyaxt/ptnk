#!/bin/bash
sync; sync; sync; bash -c 'echo 3 > /proc/sys/vm/drop_caches '
opcontrol --setup --vmlinux=/boot/vmlinux-3.0.0
opcontrol --reset
opcontrol -s
LD_LIBRARY_PATH=/usr/lib/debug:$LD_LIBRARY_PATH nice -20 $@
opcontrol -t
opcontrol -d
