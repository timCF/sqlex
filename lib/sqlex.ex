defmodule SQL do
    @default_pool :mp

    require Record
	Record.defrecord :result_packet, Record.extract(:result_packet, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :field, Record.extract(:field, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :ok_packet, Record.extract(:ok_packet, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :error_packet, Record.extract(:error_packet, from_lib: "emysql/include/emysql.hrl")

	def default_pool, do: @default_pool

	defp time_from_now(:now, date, time), do: time_from_now(:erlang.now, date, time)
	defp time_from_now(datetime, date, time) do
		{{yr,mth,day}, {hr,min,sec}} = :calendar.now_to_datetime(datetime)
		case {date, time} do
			{true, true} -> :io_lib.format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B", [yr,mth,day,hr,min,sec])
			{true, false} -> :io_lib.format("~4..0B-~2..0B-~2..0B", [yr,mth,day])
			{false, true} -> :io_lib.format("~2..0B:~2..0B:~2..0B", [hr,min,sec])
			_ -> 'NULL'
		end
	end

	defp to_atom(val), do: :erlang.binary_to_atom(val, :utf8)  
	defp set_defaults(dict, defaults), do: set_defaults(dict, defaults, Dict.keys(defaults))
	
	defp set_defaults(dict, _, []), do:	dict
	defp set_defaults(dict, defaults, [k|keys]) do 
		case dict[k] do
			nil -> set_defaults (Dict.put dict, k, defaults[k]), defaults, keys
			_   -> set_defaults dict, defaults, keys
		end
	end

    # Wraps stored procedure calls
    defp _call sql, pool do
    	case :emysql.execute(pool, sql) do
    		[result_packet(rows: rows, field_list: fields), ok_packet()] -> 
		        name_list = for field(name: name) <- fields, do: to_atom(name)
		        for row <- rows, do: Enum.zip(name_list, row)
			any_packet -> any_packet
    	end
    end

	def read sql, pool \\ @default_pool do
		case :emysql.execute pool, sql do
			result_packet(rows: rows, field_list: fields) -> 
				name_list = for field(name: name) <- fields, do: to_atom(name)
				for row <- rows, do: Enum.zip(name_list, row)
			error_packet(msg: msg) ->
				{:error, msg}  
		end
	end

	defp escape([]), do: []
	defp escape([?'|str]), do: [92,?'|escape(str)]
	defp escape([c|str]), do: [c|escape(str)]

	defp quote_if_needed(v) when is_integer(v), do: v
	defp quote_if_needed(v) when is_binary(v), do: [[?'|escape(:erlang.binary_to_list(v))]|[?']]
	defp quote_if_needed(v) when is_list(v) do
		v |> Enum.map(&quote_if_needed/1)
	end

    defp prep_argument(nil), do: 'NULL'
    defp prep_argument(true), do: 'true'
    defp prep_argument(false), do: 'false'
    defp prep_argument(:now), do: prep_argument(:erlang.now)
    defp prep_argument({:datetime, datetime}), do: time_from_now(datetime, true, true)
    defp prep_argument({:date, date}), do: time_from_now(datetime, true, false)
    defp prep_argument({:time, time}), do: time_from_now(datetime, false, true)
    defp prep_argument(:null), do: 'NULL'
    defp prep_argument(:undefined), do: 'NULL'
    defp prep_argument(arg) when is_list(arg), do: [[?(| String.to_char_list Enum.join quote_if_needed(arg), "," ]|[?)]]
    defp prep_argument(arg) when is_binary(arg), do: [[?'|escape(String.to_char_list(arg))]|[?']]
    defp prep_argument(arg) when is_atom(arg), do: [[?'|escape(Atom.to_char_list(arg))]|[?']]
    defp prep_argument(arg) when is_integer(arg), do: Integer.to_char_list arg
    defp prep_argument(arg) when is_float(arg), do: Float.to_char_list arg

	defp in_query([], _), do: []
	defp in_query(sql, args) when is_binary(sql), do: in_query(:erlang.binary_to_list(sql), args)
	defp in_query([??|sql], [arg|arg_tail]), do: [prep_argument(arg)|in_query(sql, arg_tail)]
	defp in_query([s|sql], args), do: [s|in_query(sql, args)]

	def query(sql, args), do: :erlang.list_to_binary List.flatten in_query sql, args

	def run(sql, args, pool \\ @default_pool), do: read(query(sql, args), pool)

	def call(sql, args, pool \\ @default_pool), do: _call(query(sql, args), pool)

	def execute(sql, args \\ [], pool \\ @default_pool), do: :emysql.execute(pool, query(sql, args))

	def check_transaction({:rollback, list, ok_packet()}), do: {:rollback, list}
	def check_transaction({:rollback_failed, list, _}), do: {:rollback_failed, list}
	def check_transaction([]), do: false
	def check_transaction([ok_packet()]), do: true
	def check_transaction(ok_packet()), do: true
	def check_transaction([_interim|tail]), do: check_transaction(tail)

	def init(args, pool \\ @default_pool), do: init_pool(args, pool)
	def init_pool args_original, pool \\ @default_pool do
		defaults = [
			size: 5,
			user: 'root',
			password: '',
			host: 'localhost',
			port: 3306,
			database: 'test',
			encoding: :utf8
		]
		pool = Keyword.get(args_original, :pool, pool)
		args = set_defaults(Keyword.delete(args_original, :pool), defaults)

		:ok = :emysql.add_pool(pool, args)
	end
end

defmodule SQL.Transaction do
	require Record
	Record.defrecord :emysql_connection, Record.extract(:emysql_connection, from_lib: "emysql/include/emysql.hrl")
	def run(pool_id, sql, timeout \\ 1200000) do
		connection = :emysql_conn_mgr.wait_for_connection(pool_id)
		monitor_work(connection, timeout, [connection, sql, []])
	end
	case Kernel.function_exported? Kernel, :send, 2 do
		false ->
			defp send_msg(pid, value) do
				pid <- value
			end
		true ->
			defp send_msg(pid, value) do
				send(pid, value)
			end
	end
	def monitor_work(connection, timeout, args) do
		connection = case :emysql_conn.need_test_connection(connection) do
			true ->
				:emysql_conn.test_connection connection, :keep
			false ->
				connection
		end
		parent = self
		{pid, mref} =:erlang.spawn_monitor fn ->
			:erlang.put :query_arguments, args
			send_msg(parent, { self, apply(&:emysql_conn.execute/3, args)})
		end
		receive do
			{:'DOWN', ^mref, :process, {:_, :closed}} ->
				case :emysql_conn.reset_connection :emysql_conn_mgr.pool, connection, :keep do
					newconnection = emysql_connection() ->
						[_|otherargs] = args
						monitor_work(newconnection, timeout, [newconnection|otherargs])
					{:error, failedreset} ->
						:erlang.exit {:connection_down, {:and_conn_reset_failed, failedreset}}
				end
			{:'DOWN', ^mref, :process, ^pid, reason} ->
				case :emysql_conn.reset_connection :emysql_conn_mgr.pools, connection, :pass do
					{:error, failedreset} ->
						:erlang.exit {reason, {:and_conn_reset_failed, failedreset}}
					_ ->
						:erlang.exit {reason, {}}
				end
			{^pid, result} ->
				:erlang.demonitor mref, [:flush]
				:emysql_conn_mgr.pass_connection connection
				#
				# at this point we need to check transaction result
				# and issue rollback if there was an error...
				#
				case SQL.check_transaction result do
					true -> result
					false -> 
						{:rollback, result, monitor_work(connection, timeout, [connection, "rollback;", []])}
				end
			after timeout ->
				:erlang.demonitor mref, [:flush]
				:erlang.exit pid, :kill
				:emysql_conn.reset_connection :emysql_conn_mgr.pools, connection, :pass
				:erlang.exit :mysql_timeout
		end
	end

end
