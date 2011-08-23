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

  it "should be able to handle table with a secondary index" do
    q "drop table if exists s"
    q <<-END
      create table s (
        i int primary key,
        i2 int,
        v varchar(32),
        key (i2)
      ) ENGINE=myptnk;
    END

    (0..9).each do |x|
      q %Q{insert into s values (#{x}, #{10-x}, '#{x}')}
    end

    # secondary key should be used in select
    q("describe select * from s where i2 = 1").first[:key].should eq("i2")

    (0..9).each do |x|
      r = q("select v from s where i2 = #{10-x}")
      r.count.should eq(1)
      r.first[:v].should eq(x.to_s)
    end
  end

  it "should be able to handle table with a compound key index" do
    q "drop table if exists c"
    q <<-END
      create table c(
        i int primary key,
        a int,
        b int,
        key kc (a, b)
      ) ENGINE=myptnk;
    END

    (0..9).each do |x|
      q %Q{insert into c values (#{x}, #{x*2}, #{x*3})}
    end

    # compound key should be used in select
    q("describe select * from c where a = 2 and b = 4").first[:key].should eq("kc")
    q("describe select * from c where a = 2").first[:key].should eq("kc")

    # full key search
    (0..9).each do |x|
      r = q("select i from c where a = #{x*2} and b = #{x*3}")
      r.count.should eq(1)
      r.first[:i].should eq(x)
    end

    # partial key search
    (0..9).each do |x|
      r = q("select i from c where a = #{x*2}")
      r.count.should eq(1)
      r.first[:i].should eq(x)
    end
  end

  it "should be able to handle varchar keys" do
    q "drop table if exists vck"
    q <<-END
      create table vck(
        k varchar(8) primary key,
        v int 
      ) ENGINE=myptnk;
    END
    
    (0..9).each do |x|
      q %Q{insert into vck values ('#{x}', #{x})}
    end

    (0..9).each do |x|
      r = q("select v from vck where k = '#{x}'")
      r.count.should eq(1)
      r.first[:v].should eq(x)
    end
  end

  it "should be able to handle compound key w/ varchar" do
    q "drop table if exists vck2"
    q <<-END
      create table vck2(
        k varchar(8),
        k2 int,
        primary key (k, k2)
      ) ENGINE=myptnk;
    END
    
    (0..9).each do |x|
      q %Q{insert into vck2 values ('#{x}', #{x})}
    end

    # full key search
    (0..9).each do |x|
      r = q("select k2 from vck2 where k = '#{x}' and k2 = #{x}")
      r.count.should eq(1)
      r.first[:k2].should eq(x)
    end

    # partial key search
    (0..9).each do |x|
      r = q("select k2 from vck2 where k = '#{x}'")
      r.count.should eq(1)
      r.first[:k2].should eq(x)
    end
  end

  it "should be able to handle cursor" do
    q "drop table if exists r"
    q <<-END
      create table r (
        k int primary key,
        v varchar(8)
      ) ENGINE=myptnk;
    END

    (0..9).each do |x|
      q %Q{insert into r values (#{x}, '#{x}')}
    end

    q <<-END
      create procedure curfirst(out tk int, out tv varchar(8))
      begin
        declare cur cursor for select * from r;

        open cur;
        fetch cur into tk, tv;
        close cur;
      end
    END

    q "call curfirst(@k, @v)"
    q("select @k, @v").first.should eq({:@k => 0, :@v => '0'})

    q <<-END
      create procedure cursecond(out tk int, out tv varchar(8))
      begin
        declare cur cursor for select * from r;

        open cur;
        fetch cur into tk, tv;
        fetch cur into tk, tv;
        close cur;
      end
    END

    q "call cursecond(@k, @v)"
    q("select @k, @v").first.should eq({:@k => 1, :@v => '1'})
  end

end
