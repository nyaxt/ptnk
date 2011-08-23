$: << "../rubyext/"
require 'ptnk'

db = Ptnk::DB.new(ARGV[0] || '')
tx = Ptnk::DB::Tx.new(db)

p tx.table_get_names

tx.abort
