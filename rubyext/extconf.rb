#!/usr/bin/ruby
require 'mkmf'

extension_name = 'ptnk'

find_header("ptnk.h", '..')
find_library('ptnk', '', '../build/dbg')
find_library('boost_thread', '')

dir_config(extension_name)
create_makefile(extension_name)
