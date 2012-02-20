#!/usr/bin/ruby

require 'rainbow'
require 'fileutils'

def stage(name)
  puts "* #{name}".foreground(:magenta).bright
end
def substage(str)
  puts "  - #{str}".foreground(:blue).bright
end
def subresult(str)
  puts "  -> #{str}".foreground(:green).bright
end
def msg(str)
  puts "#{str}".foreground(:cyan)
end
def warn(str)
  puts "W #{str}".foreground(:yellow)
end
def err(str)
  puts "E #{str}".foreground(:red)
end

def resputs(str='')
  unless $DRYRUN
    $DBIO.puts str
  end
end

def update_build
  stage "updating build"
  system("./waf build_rel")
  raise 'build failed abnormally' unless $?.exited?
  raise "build failed with exit status #{$?.exitstatus}" if $?.exitstatus != 0
end

class Bench

  attr_accessor :name
  
  def initialize(opts = {})
    raise 'prog is required' unless opts[:prog]
    @prog = opts[:prog]
    @args = opts[:args] || ""
    @out = []
    @name = opts[:name] || "noname"

    unless @prog[0] == '/'
      @prog = File.expand_path(@prog, $PROGBASEDIR)
    end
  end

  def run_once
    substage "running bench: #{@prog} #{@args}"

    ret = nil
    IO.popen("#{@prog} #{@args}") do |io|
      while l = io.gets
        if l =~ /^#/
          puts l
          next
        end
        if l =~ /^RESULT\s+([\w\/]+)\s+([\d\.]+)\s+(.*)$/
          ret = $2.to_f
          l = "RESULT\t#{@name}\t#{$2}\t#{$3}\n" 
        end
        puts l
        @out << l
      end
    end
    unless $?.exited? and $?.exitstatus == 0
      err "bench prog abnormally terminated: #{$?.inspect}"
      @out = []
      ret = nil
    end
    subresult "#{ret}"

    ret
  end

  def run
    $MINREPEAT.times do
      run_once
    end
  end

  def result
    @out.join
  end

end

class BasicBench < Bench

  def initialize(opts = {})
    opts[:prog] ||= $FORCE_PROG || "./build/rel/ptnk_bench"
    raise 'tmpdir is required' unless opts[:tmpdir]
    @tmpdir = opts[:tmpdir]+"/btmp"

    super opts
    @args += " --sync " if opts[:sync]
    @tgtfile = File.expand_path("bench", @tmpdir)
    @args += " #{@tgtfile}"
  end

  def run_once
    substage "mkdir tmpdir: #{@tmpdir}"
    FileUtils.mkdir_p(@tmpdir) rescue nil

    super

    substage "delete tmpdir: #{@tmpdir}"
    FileUtils.remove_entry_secure(@tmpdir)
  end

end

require 'optparse'
op = OptionParser.new

$PROGBASEDIR = File.expand_path("..", File.dirname(__FILE__))
$MINREPEAT = 3
$REV=`git show -s --pretty=%h`.chomp

op.on('--force-prog PROG') {|v| $FORCE_PROG = v; $REV = '@'+File.basename(v).gsub('_bench', '') }
op.on('-n MINREPEAT_NUM') {|v| $MINREPEAT = v.to_i }
op.on('-d') {|v| $DRYRUN = true }
op.on('-i REVSTRDESC') {|v| $REV += v }
op.on('-f') {|v| $REV =~ /^(\d+)/; $REV = $1}

op.parse!(ARGV)

if $REV =~ /\+$/
  err "need change desc for benchmarking on uncommitted rev (-i desc)"
  exit 1
end

warn "DRYRUN specified! will not be recording results to db" if $DRYRUN

# setup bench runners
SSD_PATH='/home/kouhei/work/ssd2/bench'
RAID_PATH='/home/kouhei/work/raid'
MEM_PATH='/home/kouhei/work/mem'

BASICR_ARGS = "--numPreW=1000000 --numW=0 --numR=100"
RAND_ARGS = "--random"
SMALLW_ARGS = "--numtx=100000 --numW=1"

BENCHS={
  "BasicWrite:Mem" => BasicBench.new(:tmpdir => MEM_PATH),
  "BasicWrite:SSD" => BasicBench.new(:tmpdir => SSD_PATH),
  # "BasicWrite:RAID" => BasicBench.new(:tmpdir => RAID_PATH),
  "BasicWrite:SSDSync" => BasicBench.new(:tmpdir => SSD_PATH, :sync => true),
  # "BasicWrite:RAIDSync" => BasicBench.new(:tmpdir => RAID_PATH, :sync => true),
  "RandWrite:Mem" => BasicBench.new(:tmpdir => MEM_PATH, :args => RAND_ARGS),
  "RandWrite:SSD" => BasicBench.new(:tmpdir => SSD_PATH, :args => RAND_ARGS),
  # "RandWrite:RAID" => BasicBench.new(:tmpdir => RAID_PATH, :args => RAND_ARGS),
  "RandWrite:SSDSync" => BasicBench.new(:tmpdir => SSD_PATH, :sync => true, :args => RAND_ARGS),
  # "RandWrite:RAIDSync" => BasicBench.new(:tmpdir => RAID_PATH, :sync => true, :args => RAND_ARGS),
  "BasicRead:Mem" => BasicBench.new(:tmpdir => MEM_PATH, :args => BASICR_ARGS),
  "BasicRead:SSD" => BasicBench.new(:tmpdir => SSD_PATH, :args => BASICR_ARGS),
  # "BasicRead:RAID" => BasicBench.new(:tmpdir => RAID_PATH, :args => BASICR_ARGS),
  "SmallWrite:Mem" => BasicBench.new(:tmpdir => MEM_PATH, :args => SMALLW_ARGS),
  "SmallWrite:SSD" => BasicBench.new(:tmpdir => SSD_PATH, :args => SMALLW_ARGS),
  # "SmallWrite:RAID" => BasicBench.new(:tmpdir => RAID_PATH, :args => SMALLW_ARGS),
  "SmallWrite:SSDSync" => BasicBench.new(:tmpdir => SSD_PATH, :sync => true, :args => SMALLW_ARGS),
  # "SmallWrite:RAIDSync" => BasicBench.new(:tmpdir => RAID_PATH, :sync => true, :args => SMALLW_ARGS),
  }
BENCHS.each do |name,b|
  b.name = name
end
require_relative '_aliases'

begin
  update_build

  unless $DRYRUN
    $DBIO = File.open("benchresult.#{`hostname`.chomp}", 'a')
    $DBIO.sync = true
  end

  benchs = BENCHS.values
  unless ARGV.empty?
    filter = ARGV.map {|v| Regexp.new(v)}

    benchs.keep_if do |t|
      match = false

      for f in filter
        match = true if t.name =~ f or ALIASES_REV[t.name] =~ f
      end

      match
    end
  end

  if benchs.empty?
    err "no bench to run"
    exit 1
  end

  stage "running benches"
  msg "going to run benchs:\n#{benchs.map {|b| "- #{b.name}\n"}.join}"
  
  resputs "### bench session start #{Time.now.strftime("%D %T")}"
  resputs "REV #{$REV}"
  benchs.each do |bench|
    bench.run

    resputs bench.result
    resputs
  end
  resputs "### bench session end #{Time.now.strftime("%D %T")}"
  resputs

  stage "done!"
end
