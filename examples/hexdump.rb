#!/usr/bin/ruby

def hexdump(bytes)
  ret = ''

  i = 0
  while part = bytes[i, 8] and not part.empty?
    8.times do |j|
      if part[j]
        ret << "%02x " % part[j].ord
      else
        ret << "   "
      end
    end
    ret << "| "
    8.times do |j|
      c = part[j]
      if c
        c = '?' if c.ord < 0x20 or 0x7e < c.ord
        ret << c
      else
        # nop
      end
    end
    ret << "\n"

    i += 8 
  end

  ret
end

if $0 == __FILE__
  bytes = ''
  (0..255).each do |c|
    bytes << c.chr
  end
  # p hexdump(bytes).inspect
  puts hexdump(bytes)
end
