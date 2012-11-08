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
    idle_timer,
    schema
}).

%% API
-export([open/2
        , close/1
        , exec/4
        , exec/5
        , read_block/3
        , run_cmd/3
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

close({?MODULE, Pid}) -> gen_server:cast(Pid, stop).

exec(SeCo, StmtStr, Schema,              Ctx) -> exec(SeCo, StmtStr, 0, Schema, Ctx).
exec(SeCo, StmtStr, BufferSize, Schema,  Ctx) -> run_cmd(exec, [SeCo, StmtStr, BufferSize, Schema], Ctx).
read_block(SeCo, StmtRef,                {?MODULE, Pid}) -> run_cmd(read_block, [SeCo, StmtRef], {?MODULE, Pid}).
run_cmd(Cmd, Args,    {?MODULE, Pid}) when is_list(Args) -> call(Pid, list_to_tuple([Cmd|Args])).

call(Pid, Msg) ->
    gen_server:call(Pid, Msg, ?IMEM_TIMEOUT).

init([Type, Opts]) ->
    case connect(Type, Opts) of
        {ok, Connect, Schema} ->
            io:format(user, "started ~p ~p connected to ~p~n", [?MODULE, self(), {Type, Opts}]),
            Timer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {ok, #state{connection=Connect, schema=Schema, conn_param={Type, Opts}, idle_timer=Timer}};
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
    Sess = erlimem_session:open(tcp, {localhost, 8124, "Mnesia"}),
%%    Nodes = Sess:imem_nodes(),
    Sess:close().
%    ?assertEqual(length(Nodes), 4).

tcp_table_test() ->
    erlimem:start(),
    Schema = "Mnesia",
    SeCo = {},
    Sess = erlimem_session:open(tcp, {localhost, 8124, Schema}),
    Res = Sess:exec(SeCo, "create table def (col1 int, col2 char);", Schema),
    io:format(user, "Create ~p~n", [Res]),
    Res0 = insert_range(SeCo, Sess, 24, "def", Schema),
    io:format(user, "insert ~p~n", [Res0]),
    {ok, Clms, Ref} = Sess:exec(SeCo, "select * from def;", 10, Schema),
    io:format(user, "select ~p~n", [{Clms, Ref}]),
    read_all_blocks(SeCo, Sess, Ref),
    ok = Sess:exec(SeCo, "drop table def;", Schema),
    io:format(user, "drop table~n", []),
    Sess:close().

insert_range(_SeCo, _Sess, 0, _TableName, _Schema) -> ok;
insert_range(SeCo, Sess, N, TableName, Schema) when is_integer(N), N > 0 ->
    Sess:exec(SeCo, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", Schema),
    insert_range(SeCo, Sess, N-1, TableName, Schema).

read_all_blocks(SeCo, Sess, Ref) ->
    {ok, Rows} = Sess:read_block(SeCo, Ref),
    io:format(user, "read_block ~p~n", [Rows]),
    case Rows of
        [] -> ok;
        _ -> read_all_blocks(SeCo, Sess, Ref)
    end.
