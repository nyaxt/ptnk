require_relative "../rubyext/ptnk"

if ARGV.size != 3
  puts "Usage: #{__FILE__} dbprefix tableid key"
  exit 1
end

dbprefix, tableid, key = *ARGV

db = Ptnk::DB.new(dbprefix, Ptnk::OPARTITIONED)

tx = Ptnk::DB::Tx.new(db)

t = Ptnk::Table.new(tableid)
puts tx.get(t, key)

tx.abort!

db.close
