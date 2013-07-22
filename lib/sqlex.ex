defmodule SQL do
	defrecord :result_packet, Record.extract(:result_packet, from: "deps/emysql/include/emysql.hrl")
	defrecord :field, Record.extract(:field, from: "deps/emysql/include/emysql.hrl")
	defp to_atom(val), do: :erlang.binary_to_atom(val, :utf8)  
	def read sql do
		:result_packet[rows: rows, field_list: fields]  = :emysql.execute :mp, sql
		name_list = lc :field[name: name] inlist fields, do: to_atom name
		lc row inlist rows, do: Enum.zip(name_list, row)
	end
	def init_pool do
		:ok = :emysql.add_pool :mp, 5, 'root', '', 'lotod3', 3306, 'bm', :utf8
	end
end
