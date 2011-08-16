$: << "../rubyext/"
require 'ptnk'

db = Ptnk::DB.new(ARGV[0] || '')
db.put("hello", "world")
p db.get("hello")
