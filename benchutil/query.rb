#!/usr/bin/ruby1.9.1

require 'pp'
require 'tmpdir'
require 'rainbow'
require_relative '_aliases'

def parse(host)
  benches = []
  special_benches = []
  types = []

  curr_bench = nil
  File.open("benchresult.#{host}", 'r').each_line do |line|
    case line
    when /^HGREV\s+(.*)$/
      hgrev = $1
      curr_bench = benches.find{|b| b[:hgrev] == hgrev}
      unless curr_bench
        curr_bench = {:hgrev => hgrev, :data => Hash.new {|h,k| h[k] = []}}
        benches << curr_bench
      end
    when /^RESULT\s+([\w\:]+)\s+([\d\.]+)\s+(.*)$/
      type = $1
      result = $2
      extra = $3
      curr_bench[:data][type] << [result, extra]
      types << type unless types.include?(type)
    else
      # skip
    end
  end

  [benches, types]
end

def filter_best_result(benches)
  benches.each do |bench|
    bench[:bestdata] = Hash[bench[:data].map do |type, trial|
      trial = trial.min_by {|result, _| result.to_f} 

      [type, trial]
    end]
  end

  benches
end

def separate_special(benches)
  specials = []
  
  benches.keep_if do |b|
    if b[:hgrev] =~ /^@/
      specials << b
      false
    else
      true
    end
  end

  specials
end

def limit_num(benches, n)
  n = n.to_i
  if benches.size < n
    benches
  else
    benches[-n..-1]
  end
end

def make_rel(base, benches)
  basebd = base[:bestdata]
  basebd = Marshal.load(Marshal.dump(basebd)) # deep clone
  
  benches.each do |b|
    b.delete(:data)
    bd = b[:bestdata]
    bd.each_key do |k|
      if basebd[k]
        result, extra = bd[k]
        baseresult, _ = basebd[k]

        result = "%f" % (baseresult.to_f / result.to_f)

        bd[k] = [result, '']
      else
        bd.delete(k) 
      end
    end
  end
end

def format_table(benches, types, opts)
  puts ' '*17+types.map {|t|
    (ALIASES_REV[t] || t)[0,8].rjust(8).foreground(:yellow)
  }.join(' ')

  sepline_done = false

  alt = false
  benches.each do |bench|
    if (not sepline_done) and bench[:hgrev] =~ /^@/
      puts ('-'*80).foreground(:blue)
      sepline_done = true
    end

    line = "#{bench[:hgrev][0,15].ljust(15)} :"+types.map {|t|
      bd = bench[:bestdata][t]

      unless bd
        #01234567
        '     n/a'
      else
        if opts[:percent]
          bdf = bd[0].to_f
          bds = ("%.2f%" % (bdf * 100)).rjust(8)
          if bdf > 1.02
            bds.foreground(:red)
          elsif bdf < 0.98
            bds.rjust(8).foreground(:blue)
          else
            bds.rjust(8)
          end
        else
          bd[0][0,8].rjust(8)
        end
      end
    }.join(' ')

    # line = line.bright if alt
    alt = !alt 

    puts line
  end
end

def format_plotdata(io, benches, type)
  benches.each do |bench|
    bd = bench[:bestdata][type]

    io.print %Q{"#{bench[:hgrev]}"\t}
    if bd
      io.puts bd.join("\t")
    else
      io.puts "0 "*10
    end
  end
end

COL=[
'#00cc00',
'#00ffff',
'#ff00ff',
'#0033ff',
'#ff3333',
'#ffff00',
'#ffff33',
'#33ff33',
'#3300ff',
'#3366ff',
]

def format_bargraph(io, td, benches, types, opts={})
  maxh = opts[:maxh]
  nbar = types.size 
  barw = opts[:barw] || (0.7 / nbar)

  io.puts <<-END
set terminal png small size 600,800
set output "bench.png"
set ylabel "sec"
set ytics 5
set mytics 5
set grid y
set xtics auto font "DejaVuSansMono,8"
set xtics rotate by -90
set style data boxes
set style fill solid 0.2
set boxwidth #{barw-0.01} relative
  END
  io.puts "set yrange [0:#{maxh}]" if maxh
  
  offx = -(nbar.to_f-1)/2*barw
  idx = 0
  plots = types.map do |type|
    datafile = td+"/#{idx}.data"
    File.open(datafile, 'w') do |file|
      format_plotdata(file, benches, type)
    end

    ret = %Q{"#{datafile}"}
    ret += " using ($0+#{offx}):2"
    if idx == nbar/2
      ret += ":xticlabels(1)"
    end
    ret += %Q{ title "#{type}"}
    ret += %Q{ lc rgb "#{COL[idx]}"} if COL[idx]
    ret += %Q{ fs solid 0.5}

    offx += barw
    idx += 1

    ret
  end

  io.puts "plot "+plots.join(", ")
end

def gnuplot_bargraph(benches, types, opts)
  Dir.mktmpdir do |td|
  # begin td = 'plottmp'
    gpfile = "#{td}/graph.gnuplot"
    File.open(gpfile, 'w') do |f|
      format_bargraph(f, td, benches, types, opts)
    end

    ENV['GDFONTPATH']='/usr/share/fonts/truetype/ttf-dejavu'
    `gnuplot #{gpfile}`
  end
end

# parse opts
opts = {}
ARGV.keep_if do |v|
  if v=~/(\w+)=(.*)/
    opts[$1.to_sym]=$2

    false
  else
    true # must be bench type filter
  end
end

benches, types = parse(opts[:host] || `hostname`.chomp)

# parse bench type filters
unless ARGV.empty?
  filter = ARGV.map {|v| Regexp.new(v)}

  types.keep_if do |t|
    match = false

    for f in filter
      match = true if t =~ f or ALIASES_REV[t] =~ f
    end

    match
  end
end

if types.empty?
  puts "no valid benchmark type specified"
  exit 1
end

# p types

filter_best_result(benches)
special_benches = separate_special(benches)

benches = limit_num(benches, opts[:limit] || 5)

if opts[:rel_to]
  opts[:percent] = true if opts[:percent].nil?
  base = (benches + special_benches).find {|b| b[:hgrev] =~ Regexp.new(opts[:rel_to]) }
  unless base
    puts "E no benchshot matching \"#{opts[:rel_to]}\" found"
    exit 1
  end
  puts "displaying benchresults relative to \"#{base[:hgrev]}\""
  make_rel(base, (benches + special_benches))
end

benches += special_benches unless opts[:nospec]
format_table(benches, types, opts)
gnuplot_bargraph(benches, types, opts)
