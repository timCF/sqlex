defmodule SQL do
    import Inutils, only: [zero_pad: 1]
    require Decimal
    require Record
    @default_pool :mp

	Record.defrecord :result_packet, Record.extract(:result_packet, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :field, Record.extract(:field, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :ok_packet, Record.extract(:ok_packet, from_lib: "emysql/include/emysql.hrl")
	Record.defrecord :error_packet, Record.extract(:error_packet, from_lib: "emysql/include/emysql.hrl")
	defp to_atom(val), do: :erlang.binary_to_atom(val, :utf8)  
	

	def date_to_string({:datetime, {{year, month, day}, {hour, minute, second}}}) do
		"#{year}-#{zero_pad month}-#{zero_pad day} #{zero_pad hour}:#{zero_pad minute}:#{zero_pad second}"
	end

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
        [result_packet(rows: rows, field_list: fields), ok_packet()] = :emysql.execute pool, sql
        name_list = for field(name: name) <- fields, do: to_atom(name)
        for row <- rows, do: Enum.zip(name_list, row)
    end

	def read sql, pool \\ @default_pool do
		result_packet(rows: rows, field_list: fields)  = :emysql.execute pool, sql
		name_list = for field(name: name) <- fields, do: to_atom(name)
		for row <- rows, do: Enum.zip(name_list, row)
	end

	defp escape([]), do: []
	defp escape([?'|str]), do: [92,?'|escape(str)]
	defp escape([c|str]), do: [c|escape(str)]

	defp quote_if_needed(v) when is_integer(v), do: v
	defp quote_if_needed(v) when is_binary(v), do: [[?'|escape(:erlang.binary_to_list(v))]|[?']]
	defp quote_if_needed(v) when is_list(v) do
		v |> Enum.map(&quote_if_needed/1)
	end

	defp prep_argument(arg) when is_list(arg), do: [[?(| :erlang.binary_to_list Enum.join quote_if_needed(arg), "," ]|[?)]]
	defp prep_argument(arg) when is_binary(arg), do: [[?'|escape(:erlang.binary_to_list(arg))]|[?']] 
	defp prep_argument(arg) when is_integer(arg), do: :erlang.integer_to_list arg
	defp prep_argument(arg) when is_float(arg), do: :erlang.float_to_list arg
    defp prep_argument(arg = %Decimal{}), do: to_char_list(Decimal.to_string arg)
	defp prep_argument(nil), do: 'NULL'
	defp prep_argument(:undefined), do: 'NULL'

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

	def init(args), do: init_pool args
	def init_pool args_original do
		defaults = [pool: @default_pool, size: 5, user: 'root', password: '', host: 'localhost', port: 3306, db: 'test']
		args = set_defaults args_original, defaults
		:ok = :emysql.add_pool args[:pool], args[:size], args[:user], args[:password], args[:host], args[:port], args[:db], :utf8
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
