require_relative "../rubyext/ptnk"

db = Ptnk::DB.new(ARGV[0] || '')
tx = Ptnk::DB::Tx.new(db)

tx.table_get_names.each do |tn|
  puts "Table: #{tn.inspect}"
  t = Ptnk::Table.new(tn)

  c = tx.cursor_front(t)
  if c
    begin
      p c.get
    end while c.next
  end
end

tx.abort!
