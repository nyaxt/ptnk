# gtest.py by nyaxt based on tanakh's waf-unittest
# below is waf-unittest's copyright notice
"""
Copyright (c)2011, Hideyuki Tanaka

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following
disclaimer in the documentation and/or other materials provided
with the distribution.

* Neither the name of Hideyuki Tanaka nor the names of other
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

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
	o.add_option('--buildtest', action = 'store_true', default = False, help = 'Build unit tests (but not exec)')

@feature('gtest')
@before('process_rule')
def test_remover(self):
	if not Options.options.check and not Options.options.buildtest:
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
		if not Options.options.check: return 0

		filename = self.inputs[0].abspath()
		
		self.lock.acquire()

		proc = Utils.subprocess.Popen([filename])
		proc.wait()

		self.lock.release()

		return 0 if proc.returncode == 0 else 1
