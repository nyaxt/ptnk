require 'mysql2'

def q(s)
  $db.query(s)
end

def qf(s)
  q(s)
rescue Mysql2::Error
  # just ignore
end

describe "MyPTNK" do
  before(:all) do
    $db = Mysql2::Client.new(:socket => "/home/kouhei/work/myvar/socket", :username => 'root')
    $db.query_options.merge!(:symbolize_keys => true)

    qf "install plugin myptnk soname 'ha_myptnk.so'"

    q "drop database if exists test"
    q "create database test"
    q "use test"
  end

  it "should be able to handle very simple table without keys" do
    q "drop table if exists t"
    q "create table t(a int) ENGINE=myptnk"

    r = q("select * from t")
    r.count.should eq(0)

    q "insert into t values (0)"

    r = q("select * from t")
    r.count.should eq(1)
    r.first.should eq({a: 0})

    q "insert into t values (1)"
    q "insert into t values (2)"

    r = q("select * from t")
    r.count.should eq(3)
    r.to_a.should eq([{a: 0}, {a: 1}, {a: 2}])
  end

  it "should be able to handle table with multiple columns" do
    q "drop table if exists names"
    q "create table names(f varchar(32), m varchar(8), l varchar(64)) ENGINE=myptnk;"

    r = q("select * from names")
    r.count.should eq(0)

    q %Q{insert into names values ('A', 'B', 'C')}
    r = q("select * from names")
    r.count.should eq(1)
    r.to_a.should eq([{f: 'A', m: 'B', l: 'C'}])

    q %Q{insert into names values ('John', NULL, 'Smith')}
    r = q("select * from names")
    r.count.should eq(2)
    r.to_a.should eq([{f: 'A', m: 'B', l: 'C'}, {f: 'John', m: nil, l: 'Smith'}])
  end

  it "should be able to handle table with single index" do
    q "drop table if exists i"
    q <<-END
      create table i (
        k int primary key,
        v varchar(8) 
      ) ENGINE=myptnk;
    END

    q("select * from i").count.should eq(0)

    (0..9).each do |x|
      q %Q{insert into i values (#{x}, '#{x}')}
    end

    (0..9).each do |x|
      r = q("select v from i where k = #{x}")
      r.count.should eq(1)
      r.first[:v].should eq(x.to_s)
    end

    # primary key should be used in select
    q("describe select * from i where k = 1").first[:key].should eq("PRIMARY")
  end

end
