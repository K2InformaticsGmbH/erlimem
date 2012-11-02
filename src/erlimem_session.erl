-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    status=closed,
    port,
    pool,
    session_id,
    statements = [],
    connection = {type, handle},
    conn_param,
    idle_timer
}).

%%-record(statement, {
%%    ref,
%%    handle,
%%    max_rows,
%%    row_top = 0,
%%    row_bottom = 0,
%%    columns,
%%    results,
%%    use_cache = false,
%%    port_row_fetch_status = more,
%%    fetch_activity
%%}).

%% API
-export([%execute_sql/4,
         %execute_sql/5,
         %next_rows/1,
         %prev_rows/1,
         %rows_from/2,
         %get_buffer_max/1,
         %close/1,
         %get_columns/1,
         open/2
        , close/1
        , imem_nodes/1
        , tables/1
		, columns/2
        , read/3
        , write/3
        , delete/3
        , add_attribute/3
		, delete_table/2
        , update_opts/3
        , read_all_rows/2
		, build_table/3
        , select_rows/3
        , insert_into_table/3
        , build_table/4
		]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% @doc open new session
open(Type, Opts) ->
    {ok, Pid} = gen_server:start(?MODULE, [Type, Opts], []),
    {?MODULE, Pid}.

close(                                  {?MODULE, Pid}) -> gen_server:cast(Pid, stop).
imem_nodes(                             {?MODULE, Pid}) -> call(Pid, {imem_nodes}).
tables(                                 {?MODULE, Pid}) -> call(Pid, {tables}).
columns(TableName,                      {?MODULE, Pid}) -> call(Pid, {columns, TableName}).
read(TableName, Key,                    {?MODULE, Pid}) -> call(Pid, {read, TableName, Key}).
write(TableName, Row,                   {?MODULE, Pid}) -> call(Pid, {write, TableName, Row}).
delete(TableName, Key,                  {?MODULE, Pid}) -> call(Pid, {delete, TableName, Key}).
add_attribute(A, Opts,                  {?MODULE, Pid}) -> call(Pid, {add_attribute, A, Opts}).
delete_table(TableName,                 {?MODULE, Pid}) -> call(Pid, {delete_table, TableName}).
update_opts(Tuple, Opts,                {?MODULE, Pid}) -> call(Pid, {update_opts, Tuple, Opts}).
read_all_rows(TableName,                {?MODULE, Pid}) -> call(Pid, {read_all_rows, TableName}).
build_table(TableName, Columns,         {?MODULE, Pid}) -> call(Pid, {build_table, TableName, Columns}).
select_rows(TableName, MatchSpec,       {?MODULE, Pid}) -> call(Pid, {select_rows, TableName, MatchSpec}).
insert_into_table(TableName, Row,       {?MODULE, Pid}) -> call(Pid, {insert_into_table, TableName, Row}).
build_table(TableName, Columns, Opts,   {?MODULE, Pid}) -> call(Pid, {build_table, TableName, Columns, Opts}).

call(Pid, Msg) ->
    gen_server:call(Pid, Msg, ?IMEM_TIMEOUT).

init([Type, Opts]) ->
    case connect(Type, Opts) of
        {ok, Connect} ->
            io:format(user, "started ~p ~p connected to ~p~n", [?MODULE, self(), {Type, Opts}]),
            Timer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {ok, #state{connection=Connect, conn_param={Type, Opts}, idle_timer = Timer}};
        {error, Reason} -> {stop, Reason}
    end.

connect(tcp, {IpAddr, Port, Schema}) ->
    {ok, Ip} = inet:getaddr(IpAddr, inet),
    {ok, Socket} = gen_tcp:connect(Ip, Port, []),
    inet:setopts(Socket, [{active, false}, binary, {packet, 0}, {nodelay, true}]),
    {ok, {tcp, Socket}, Schema};
connect(rpc, {Node, Schema}) when Node == node() ->
    connect(connect_local, {Schema});
connect(connect_rpc, {Node, Schema}) ->
    {ok, {rpc, Node}, Schema};
connect(connect_local, {Schema}) ->
    {ok, {local, undefined}, Schema}.

handle_call(Msg, _From, #state{connection=Connection,idle_timer=Timer} = State) ->
    erlang:cancel_timer(Timer),
    Nodes = erlimem_cmds:exec(Msg, Connection),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Nodes,State#state{idle_timer=NewTimer}}.

handle_cast(stop, State) ->
    {stop,normal,State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    close({?MODULE, self()}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn_param={Type, Opts},idle_timer=Timer}) ->
    erlang:cancel_timer(Timer),
    io:format(user, "stopped ~p ~p disconnected from ~p~n", [?MODULE, self(), {Type, Opts}]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.


% EUnit tests --

tcp_test() ->
    erlimem:start(),
    Sess = erlimem_session:open(connect_tcp, {localhost, 8124}),
    Nodes = Sess:imem_nodes(),
    Sess:close(),
    ?assertEqual(length(Nodes), 4).

tcp_table_test() ->
    erlimem:start(),
    Sess = erlimem_session:open(connect_tcp, {localhost, 8124}),
    Nodes = Sess:imem_nodes(),
    io:format(user, "Nodes ~p~n", [Nodes]),
    Sess:close(),
    ?assertEqual(length(Nodes), 4).

