#!/usr/bin/ruby
# coding: utf-8

W = 20
tscale = 1e-4
height = 2000.0

def svg_rect(x, y, w, h, col)
  puts %Q{<rect style="fill:##{col};" x="#{x}" y="#{y}" width="#{w}" height="#{h}"/>}
end


cthr = nil
tsend = nil

last_ts = 0
last_col = "000000"

ARGF.each_line do |l|
  case l
  when /^TSEND (\d+)/
    tsend = $1.to_i
    puts <<EOS
<?xml version="1.0" standalone="no" ?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 20010904//EN" 
"http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd">

<svg width="#{W*10}" height="#{tsend * tscale}" xmlns="http://www.w3.org/2000/svg">
EOS

  when /^THREAD (\d+)/
    ts = tsend
    svg_rect cthr*W, last_ts*tscale, W, (ts-last_ts)*tscale, last_col if cthr

    cthr = $1.to_i
    last_ts = 0

  when /^(\d+), ([0-9a-fA-F]+)/
    ts = $1.to_i
    svg_rect cthr*W, last_ts*tscale, W, (ts-last_ts)*tscale, last_col

    last_ts = ts
    last_col = $2
  end
end
svg_rect cthr*W, last_ts*tscale, W, (tsend-last_ts)*tscale, last_col if cthr

puts "</svg>"
