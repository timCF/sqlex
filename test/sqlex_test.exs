Code.require_file "test_helper.exs", __DIR__

defmodule SqlexTest do
  use ExUnit.Case

  test "we can start apps" do
  	assert( Enum.all? (lc a inlist [:crypto, :emysql], do: :application.start a), fn r ->
  		{:error, {:already_started, _}} = r
  	end)	 
  end

  test "we can run query" do
  	[row] = SQL.read "select 1 as ok"
  	assert(row[:ok] == 1)
  end

  test "the truth" do
    assert(true)
  end

  test "prepare works" do
  	assert "select hello 'world'? " == SQL.query "select hello ?? ", ["world"]
  end

  test "prepare works for several args" do
  	assert "select hello, 'world' and 'joe'" == SQL.query "select hello, ? and ?", ["world", "joe"]
  end

  test "prepare works for numbers as well" do
  	assert "select hello 1980" == SQL.query "select hello ?", [1980]
  end

  test "prepare works for numbers and binaries" do
  	assert "select 'hello' 1980" == SQL.query "select ? ?", ["hello", 1980]
  end

  test "prepare works for lists" do
  	assert "select * from posts where id in (1,2,300)" == SQL.query "select * from posts where id in ?", [[1,2,300]]
  end

  test "escaping works" do
  	assert "select 'hel\\'o'" == SQL.query "select ?", ["hel'o"]
  end

  test "transaction check" do
    assert SQL.check_transaction([{:ok_packet,1,0,0,11,0,[]}, {:ok_packet,2,1,1,11,0,[]}, {:result_packet,7, []}, {:ok_packet,8,1,1,11,0,[]}, {:ok_packet,9,0,0,2,0,[]}]) == true
  end

  test "running query with args" do
  	[row] = SQL.run "select ? as ok", [1]
  	assert(row[:ok] == 1)
  end
end
