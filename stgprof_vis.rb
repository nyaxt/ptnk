#!/usr/bin/ruby
# coding: utf-8

W = 20
TSCALE = 1e-4

def svg_proepi
  puts <<EOS
<?xml version="1.0" standalone="no" ?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 20010904//EN" 
"http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd">

<svg width="#{W*8}" height="2000" xmlns="http://www.w3.org/2000/svg">
EOS
  
  yield

  puts "</svg>"
end

def svg_rect(x, y, w, h, col)
  puts %Q{<rect style="fill:##{col};" x="#{x}" y="#{y}" width="#{w}" height="#{h}"/>}
end


svg_proepi do

  cthr = nil
  tsend = nil

  last_ts = 0
  last_col = "000000"

  ARGF.each_line do |l|
    case l
    when /^TSEND (\d+)/
      tsend = $1.to_i

    when /^THREAD (\d+)/
      ts = tsend
      svg_rect cthr*W, last_ts*TSCALE, W, (ts-last_ts)*TSCALE, last_col if cthr

      cthr = $1.to_i
      last_ts = 0

    when /^(\d+), ([0-9a-fA-F]+)/
      ts = $1.to_i
      svg_rect cthr*W, last_ts*TSCALE, W, (ts-last_ts)*TSCALE, last_col

      last_ts = ts
      last_col = $2
    end
  end
  svg_rect cthr*W, last_ts*TSCALE, W, (tsend-last_ts)*TSCALE, last_col if cthr

end
