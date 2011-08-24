require_relative '../ptnk'
require 'tmpdir'

describe Ptnk::DB do

  it "should be able to put/get records" do
    db = Ptnk::DB.new('') # on memory db

    db.put("hello", "world")
    db.get("hello").should eq("world")
  end

  it "should be able to persistently store records" do
    Dir.mktmpdir do |dir|
      db = Ptnk::DB.new("#{dir}/test")
      db.put("hello", "world")
      db.close

      db2 = Ptnk::DB.new("#{dir}/test")
      db2.get("hello").should eq("world")
    end
    # db = Ptnk::DB.new
  end

  it "should be able to use transaction to store records" do
    db = Ptnk::DB.new('')

    tx = Ptnk::DB::Tx.new(db)
    tx.put("hello", "world", Ptnk::PUT_INSERT)

    db.get("hello").should be false # tx not committed yet

    tx.try_commit!.should be true
    
    db.get("hello").should eq("world")
  end

end
