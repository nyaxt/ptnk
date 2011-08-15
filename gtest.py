# gtest.py based on tanakh's waf-unittest

import os, sys, threading
from waflib.TaskGen import before, after, feature
from waflib import Options, Task, Utils, Logs, Errors

def configure(c):
	# google test (gtest)
	#   check_cxx is for latest debian env. which doesn't include gtest-config
	if c.check_cfg(
		path = 'gtest-config',
		args = '--cppflags --cxxflags --ldflags --libs',
		package = '',
		uselib_store = 'GTEST',
		mandatory=False) or \
	   c.check_cxx(	
		uselib_store = 'GTEST',
		lib='gtest',
		mandatory=False):
		c.env.LIB_GTEST.append('gtest_main')

def options(o):
	o.add_option('--check', action = 'store_true', default = False, help = 'Exec unit tests')

@feature('gtest')
@before('process_rule')
def test_remover(self):
	if not Options.options.check:
		self.meths[:] = []

@feature('gtest')
@after('apply_link')
def make_test(self):
	self.default_install_path = None
	self.create_task('gtest', self.link_task.outputs)

class gtest(Task.Task):
	color = 'PINK'
	lock = threading.Lock()

	def runnable_status(self):
		ret = super(gtest, self).runnable_status()
		if ret != Task.SKIP_ME: return ret

		return Task.RUN_ME if Options.options.check else Task.SKIP_ME

	def run(self):
		filename = self.inputs[0].abspath()
		
		self.lock.acquire()

		proc = Utils.subprocess.Popen([filename])
		proc.wait()

		self.lock.release()

		return 0 if proc.returncode == 0 else 1
