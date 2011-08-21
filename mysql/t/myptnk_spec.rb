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

    qf "drop database test"
    q "create database test"
    q "use test"
  end

  it "should be able to handle simple table" do
    q "drop table if exists t"
    q "create table t(a int primary key) ENGINE=myptnk"

    q "insert into t values (0)"

    q("select * from t").first.should eq({a: 0})
  end

end
