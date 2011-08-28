VERSION='0.0.1'
APPNAME='ptnk'

top = '.'
out = 'build'

import sys
import commands
import Utils
from waflib import Build, Options, TaskGen, Task

def options(opt):
	opt.load('compiler_cxx boost gtest')

	opt.add_option('--with-dtrace', action='store_true', default=False, help='enable DTrace/SystemTap probes', dest='dtrace')

def configure(conf):
	conf.env.append_unique('CXXFLAGS', ['-Wall', '-g'])
	conf.env.append_unique('INCLUDES', ['/home/kouhei/local/include', '/usr/local/include', '/opt/local/include'])
	conf.env.append_unique('LIBPATH', ['/home/kouhei/local/lib', '/usr/local/lib', '/opt/local/lib'])

	conf.load('compiler_cxx boost gtest')

	# DTrace / SystemTap probes
	if Options.options.dtrace:
		conf.find_program('dtrace', var='DTRACE', mandatory=True)
		conf.env["USE_DTRACE"] = True
		conf.env.append_unique('DEFINES', ['HAVE_DTRACE'])

	# posix_fallocate exist?
	conf.check_cc(fragment='''
		#include <fcntl.h>
		int main() {
			::posix_fallocate(0, 0, 0);
			return 0;
		}
		''',
		define_name = 'USE_POSIX_FALLOCATE',
		execute = False,
		msg = "Checking for posix_fallocate(2)",
		
		mandatory=False
		)

	# check for clock_gettime
	conf.check_cc(fragment='''
		#include <time.h>
		int main() {
			struct timespec ts;
			::clock_gettime(CLOCK_MONOTONIC, &ts);
			return 0;
		}
		''',
		define_name = 'USE_CLOCK_GETTIME',
		execute = False,
		msg = "Checking for clock_gettime(2)",
		
		mandatory=False
		)

	# check for mach_absolute_time
	conf.check_cc(fragment='''
		#include <stdint.h>
		#include <mach/mach_time.h>
		int main() {
			::mach_absolute_time();
			return 0;
		}
		''',
		define_name = 'USE_MACH_ABSOLUTE_TIME',
		execute = False,
		msg = "Checking for mach_absolute_time(2)",
		
		mandatory=False
		)

	# tcmalloc
	conf.check_cxx(uselib_store='TCMALLOC', lib='tcmalloc', mandatory=False)

	# pthread
	conf.check_cxx(uselib_store='PTHREAD', lib='pthread', mandatory=True)
	conf.check_cxx(uselib_store='PTHREAD', lib='rt', mandatory=False) # only on linux ?

	# boost headers
	conf.check_boost(uselib_store='BOOST')

	# boost thread
	conf.check_boost(uselib_store='BOOST_THREAD', lib='thread', mt=True)

	conf.write_config_header('ptnk_config.h', remove=True)
	conf.env.append_unique('DEFINES', ['HAVE_PTNK_CONFIG_H'])

	# make PIC
	conf.env.append_unique('CXXFLAGS', ['-fPIC'])

	#########################################################
	# variants conf. 

	dbgenv = conf.env.derive()
	relenv = conf.env.derive()

	# *** DEBUG
	conf.setenv('dbg', env=dbgenv)
	conf.env.append_unique('CXXFLAGS', ['-O0'])
	conf.env.append_unique('DEFINES', ['PTNK_DEBUG'])
	
	# *** RELEASE
	conf.setenv('rel', env=relenv)
	conf.env.append_unique('CXXFLAGS', ['-O4'])
	conf.env.append_unique('DEFINES', ['PTNK_USE_CUSTOM_MEMCMP']) # FIXME: add bench

@TaskGen.feature('link_dtrace')
@TaskGen.before('apply_link')
def make_dtrace_o(self):
	if sys.platform == 'darwin':
		# Darwin DTrace do not require obj linking
		pass
	else:
		# TODO
		# objs=[t.outputs[0]for t in getattr(self,'compiled_tasks',[])]
		# self.dtrace_o_task = self.create_task('dtrace_o', objs, )
		pass

# class dtrace_o(Task.Task):
#	color = 'PINK'
	

def build(bld):
	if not bld.variant:
		bld.fatal('no variant selected (try build_rel / build_dbg)')

	# hgnum
	# -- should be at configure, but for convenience
	# def get_hgnum():
	# 	pid, result = commands.getstatusoutput("hg id -n")
	# 	if pid == 0:	return result
	# 	else:		return 'UNKNOWN'
	# bld.env.append_unique('DEFINES', ['HGNUM="%s"' % get_hgnum()])

	bld.env = bld.all_envs[bld.variant]
	bld.env.append_unique('INCLUDES', ['..', '.'])

        bld.install_files('${PREFIX}/include', ['ptnk.h', 'ptnk/buffer.h', 'ptnk/common.h', 'ptnk/db.h', 'ptnk/exceptions.h', 'ptnk/page.h', 'ptnk/query.h', 'ptnk/toc.h', 'ptnk/types.h'], relative_trick=True)

	b_ptnk = bld.stlib(
		target = 'ptnk',
		install_path = '${PREFIX}/lib',

		use = 'TCMALLOC PTHREAD BOOST_THREAD',
		source = '''
		ptnk/hash.cpp
		ptnk/fileutils.cpp
		ptnk/buffer.cpp
		ptnk/page.cpp
		ptnk/pageio.cpp
		ptnk/pageiomem.cpp
		ptnk/partitionedpageio.cpp
		ptnk/btree.cpp
		ptnk/compmap.cpp
		ptnk/tpio.cpp
		ptnk/overview.cpp
		ptnk/db.cpp
		ptnk.cpp
		''',
		)

	if bld.env['USE_DTRACE']:
		b_dtrace_h = bld.new_task_gen(
			name = 'dtrace_h',
			source = 'ptnk_probes.d',
			target = 'ptnk_probes.h',
			rule = "%s -h -o ${TGT} -s ${SRC}" % (bld.env.DTRACE),
			before = "cxx"
			)

		b_ptnk.use += ' dtrace_h'
		b_ptnk.features += ['link_dtrace']

	# ptnk_test
	if not bld.env.LIB_GTEST or len(bld.env.LIB_GTEST) == 0:
		Logs.warn('gtest is not found / skipping ptnk_test')
	else:
		bld.program(
			target = 'ptnk_test',

			features = 'gtest',
			use = 'TCMALLOC PTHREAD BOOST_THREAD GTEST ptnk',
			source = 'ptnk_test.cpp'
			)

	# bench utils
	if bld.variant == 'rel':
		bld.program(
			target = 'ptnk_bench',

			use = 'TCMALLOC ptnk',
			source = 'ptnk_bench.cpp'
			)

		bld.program(
			target = 'ptnk_mtbench',
			
			use = 'TCMALLOC PTHREAD BOOST ptnk',
			source = 'ptnk_mtbench.cpp'
			)

	# debug utils
	bld.program(
		target = 'ptnk_dump',

		cxxflags = '-std=c++0x',
		use = 'TCMALLOC BOOST ptnk',
		source = 'ptnk_dump.cpp'
		)

	bld.program(
		target = 'ptnk_bindump',

		use = 'TCMALLOC BOOST ptnk',
		source = 'ptnk_bindump.cpp'
		)

	bld.program(
		target = 'ptnk_dumpstreak',

		use = 'TCMALLOC BOOST ptnk',
		source = 'ptnk_dumpstreak.cpp'
		)

	bld.program(
		target = 'ptnk_findroot',

		use = 'TCMALLOC BOOST ptnk',
		source = 'ptnk_findroot.cpp'
		)

	bld.program(
		target = 'ptnk_compact',

		use = 'TCMALLOC BOOST ptnk',
		source = 'ptnk_compact.cpp'
		)

from waflib.Build import BuildContext, CleanContext, InstallContext, UninstallContext

for x in 'dbg rel'.split():
	for y in (BuildContext, CleanContext, InstallContext, UninstallContext):
		name = y.__name__.replace('Context','').lower()
		class tmp(y):
			cmd = name + '_' + x
			variant = x

def build_all(ctx):
	import waflib.Options
	waflib.Options.commands.extend(['build_dbg', 'build_rel'])

def clean_all(ctx):
	import waflib.Options
	waflib.Options.commands.extend(['clean_dbg', 'clean_rel'])
