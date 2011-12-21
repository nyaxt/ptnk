#!/usr/bin/ruby
require 'mkmf'

extension_name = 'ptnk'

CONFIG['CXXFLAGS'] = (CONFIG['CXXFLAGS'] || '') + ' --std=c++0x'

find_header("ptnk.h", '..')
find_library('ptnk', '', '../build/rel')
find_library('boost_thread', '')

dir_config(extension_name)
create_makefile(extension_name)
