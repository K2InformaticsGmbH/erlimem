-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").
-include_lib("imem/include/imem_sql.hrl").

-record(state, {
    stmts       = [],
    connection  = {type, handle},
    event_pids  = [],
    buf         = {0, <<>>},
    conn_param,
    schema,
    seco,
    maxrows,
    inetmod
}).

-record(stmt, {
    fsm
}).

% session APIs
-export([ close/1
        , exec/3
        , exec/4
        , exec/5
        , run_cmd/3
        , get_stmts/1
		]).

% gen_server callbacks
-export([ start_link/3
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , add_stmt_fsm/3
        ]).

start_link(Type, Opts, Cred) ->
    ?Info("~p starting...", [?MODULE]),
    case gen_server:start_link(?MODULE, [Type, Opts, Cred], [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p", [?MODULE, Error]),
            Error
    end.

%
% interface functions
%

-spec close({atom(), pid()} | {atom(), pid(), pid()}) -> ok.
close({?MODULE, Pid}) -> gen_server:call(Pid, stop);
close({?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {close_statement, StmtRef}).

-spec exec(list(), list(), {atom(), pid()}) -> term().
exec(StmtStr, Params, Ctx) -> exec(StmtStr, 0, Ctx, Params).

-spec exec(list(), integer(), list(), {atom(), pid()}) -> term().
exec(StmtStr, BufferSize, Params, Ctx) -> run_cmd(exec, [Params, StmtStr, BufferSize], Ctx).

-spec exec(list(), integer(), fun(), {atom(), pid()}, list()) -> term().
exec(StmtStr, BufferSize, Fun, Params, Ctx) -> run_cmd(exec, [Params, StmtStr, BufferSize, Fun], Ctx).

-spec run_cmd(atom(), list(), {atom(), pid()}) -> term().
run_cmd(Cmd, Args, {?MODULE, Pid}) when is_list(Args) -> gen_server:call(Pid, [Cmd|Args], ?IMEM_TIMEOUT).

-spec add_stmt_fsm(pid(), {atom(), pid()}, {atom(), pid()}) -> ok.
add_stmt_fsm(StmtRef, StmtFsm, {?MODULE, Pid}) -> gen_server:call(Pid, {add_stmt_fsm, StmtRef, StmtFsm}, ?SESSION_TIMEOUT).

-spec get_stmts(list() | {atom(), pid()}) -> [pid()].
get_stmts({?MODULE, Pid}) -> gen_server:call(Pid, get_stmts, ?SESSION_TIMEOUT);
get_stmts(PidStr)         -> gen_server:call(list_to_pid(PidStr), get_stmts, ?SESSION_TIMEOUT).

%
% gen_server callbacks
%
init([Type, Opts, {User, Pswd, SessionId}]) ->
    init([Type, Opts, {User, Pswd, undefined, SessionId}]);
init([Type, Opts, {User, Pswd, NewPswd, SessionId}]) when is_binary(User), is_binary(Pswd) ->
    ?Debug("connecting with ~p cred ~p", [{Type, Opts}, {User, Pswd, SessionId}]),
    case connect(Type, Opts) of
        {ok, Connect, Schema} = Response ->
            ?Debug("Response from server connect ~p", [Response]),
            try
                SeCo = get_seco(User, SessionId, Connect, Pswd, NewPswd),
                ?Debug("~p connects ~p over ~p with ~p", [self(), User, Type, Opts]),
                InetMod = case Connect of {ssl, _} -> ssl; _ -> inet end,
                {ok, #state{connection=Connect, schema=Schema, conn_param={Type, Opts}, seco=SeCo, inetmod=InetMod}}
            catch
                _Class:Reason ->
                    ?Error("erlimem connect error : ~p stackstrace : ~p~n", [Reason, erlang:get_stacktrace()]),
                    case Connect of
                        {gen_tcp, Sock} -> gen_tcp:close(Sock);
                        {ssl, Sock} -> ssl:close(Sock);
                        _ -> ok
                    end,
                    {stop, Reason}
            end;
        {error, Reason} ->
            ?Error("erlimem connect error, reason:~n~p", [Reason]),
            {stop, Reason}
    end.

%% handle_call overloads
%%
handle_call(get_stmts, _From, #state{stmts=Stmts} = State) ->
    {reply,[S|| {S,_} <- Stmts],State};
handle_call(stop, _From, State) ->
    {stop,normal, ok, State};
handle_call({add_stmt_fsm, StmtRef, {_, StmtFsmPid} = StmtFsm}, _From, #state{stmts=Stmts} = State) ->
    erlang:monitor(process, StmtFsmPid),
    NStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, #stmt{fsm = StmtFsm}}),
    {reply,ok,State#state{stmts=NStmts}};
handle_call(Msg, From, #state{connection=Connection
                             ,schema=Schema
                             ,seco=SeCo
                             ,event_pids=EvtPids} = State) ->
    ?Debug("call ~p", [Msg]),
    [Cmd|Rest] = Msg,
    NewMsg = case Cmd of
        exec ->
            NewEvtPids = EvtPids,
            [Params|Args] = Rest,
            list_to_tuple([Cmd,SeCo|Args] ++ [[{schema, Schema}, {params, Params}]]);
        subscribe ->
            [Evt|_] = Rest,
            {Pid, _} = From,
            NewEvtPids = lists:keystore(Evt, 1, EvtPids, {Evt, Pid}),
            list_to_tuple([Cmd,SeCo|Rest]);
        _ ->
            NewEvtPids = EvtPids,
            list_to_tuple([Cmd,SeCo|Rest])
    end,
    case (catch erlimem_cmds:exec(From, NewMsg, Connection)) of
        {{error, E}, ST} ->
            ?Error("cmd ~p error~n~p~n", [Cmd, E]),
            ?Debug("~p", [ST]),
            {reply, E, State#state{event_pids=NewEvtPids}};
        _Result ->
            {noreply,State#state{event_pids=NewEvtPids}}
    end.

%% handle_cast overloads
%%  unhandled
handle_cast(Request, State) ->
    ?Error([session, self()], "unknown cast ~p", [Request]),
    {stop,cast_not_supported,State}.

%% handle_info overloads
%%
handle_info(timeout, State) ->
    ?Info("~p close on timeout", [self()]),
    {stop,normal,State};

% tcp
handle_info({Tcp, S, <<L:32, PayLoad/binary>> = Pkt}, #state{buf={0, <<>>}, inetmod=InetMod} = State) when Tcp =:= tcp; Tcp =:= ssl ->
    ?Debug("RX (~p)~n~p", [byte_size(Pkt),Pkt]),
    InetMod:setopts(S,[{active,once}]),
    ?Debug( " term size ~p~n", [L]),
    {NewLen, NewBin, Commands} = split_packages(L, PayLoad),
    NewState = process_commands(Commands, State),
    {noreply, NewState#state{buf={NewLen, NewBin}}};
handle_info({Tcp,S,Pkt}, #state{buf={Len,Buf}, inetmod=InetMod}=State) when Tcp =:= tcp; Tcp =:= ssl ->
    ?Debug("RX (~p)~n~p", [byte_size(Pkt),Pkt]),
    InetMod:setopts(S,[{active,once}]),
    {NewLen, NewBin, Commands} = split_packages(Len, <<Buf/binary, Pkt/binary>>),
    NewState = process_commands(Commands, State),
    {noreply, NewState#state{buf={NewLen, NewBin}}};
handle_info({TcpClosed,Socket}, State) when TcpClosed =:= tcp_closed; TcpClosed =:= ssl_closed ->
    ?Debug("~p tcp closed ~p", [self(), Socket]),
    {stop,normal,State};

% statement monitor events
handle_info({'DOWN', Ref, process, StmtFsmPid, Reason}, #state{stmts=Stmts}=State) ->
    [StmtRef|_] = [SR || {SR, DS} <- Stmts, element(2, DS#stmt.fsm) =:= StmtFsmPid],
    NewStmts = lists:keydelete(StmtRef, 1, Stmts),
    true = demonitor(Ref, [flush]),
    ?Debug("FSM ~p died with reason ~p for stmt ~p remaining ~p", [StmtFsmPid, Reason, StmtRef, [S || {S,_} <- NewStmts]]),
    {noreply, State#state{stmts=NewStmts}};

% mnesia events handling
handle_info({_,{complete, _}} = Evt, #state{event_pids=EvtPids}=State) ->
    case lists:keyfind(activity, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "# ~p <- ~p", [Found, Evt])
    end,
    {noreply, State};
handle_info({_,{S, Ctx, _}} = Evt, #state{event_pids=EvtPids}=State) when S =:= write;
                                                                          S =:= delete_object;
                                                                          S =:= delete ->
    Tab = element(1, Ctx),
    case lists:keyfind({table, Tab}, 1, EvtPids) of
        {_, Pid} -> Pid ! Evt;
        _ ->
            case lists:keyfind({table, Tab, simple}, 1, EvtPids) of
                {_, Pid} when is_pid(Pid) -> Pid ! Evt;
                Found ->
                    ?Debug([session, self()], "# ~p <- ~p", [Found, Evt])
            end
    end,
    {noreply, State};
handle_info({_,{D,Tab,_,_,_}} = Evt, #state{event_pids=EvtPids}=State) when D =:= write;
                                                                            D =:= delete ->
    case lists:keyfind({table, Tab, detailed}, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "# ~p <- ~p", [Found, Evt])
    end,
    {noreply, State};

% local / rpc / tcp fallback
handle_info({_Ref,{StmtRef,{Rows,Completed}}}, #state{stmts=Stmts}=State) when is_pid(StmtRef) ->
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #stmt{fsm=StmtFsm}} ->
            case {Rows,Completed} of
                {error, Result} = Error ->
                    StmtFsm:rows(Error),
                    ?Error([session, self()], "async_resp~n~p~n", [Result]),
                    {noreply, State};
                {Rows, Completed} ->
                    StmtFsm:rows({Rows,Completed}),
                    ?Debug("~p __RX__ received rows ~p status ~p", [StmtRef, length(Rows), Completed]),
                    {noreply, State};
                Unknown ->
                    StmtFsm:rows(Unknown),
                    ?Error([session, self()], "async_resp unknown resp~n~p~n", [Unknown]),
                    {noreply, State}
            end;
        false ->
            ?Error("statement ~p not found in ~p", [StmtRef, [S|| {S,_} <- Stmts]]),
            {noreply, State}
    end;
handle_info({From,Resp}, #state{stmts=Stmts}=State) ->
    case Resp of
            {error, Exception} ->
                ?Error("to ~p throw~n~p~n", [From, Exception]),
                gen_server:reply(From,  {error, Exception}),
                {noreply, State};
            {ok, #stmtResult{stmtRef  = StmtRef} = SRslt} ->
                ?Debug("RX ~p", [SRslt]),
                %Rslt = {ok, SRslt, {?MODULE, StmtRef, self()}},
                Rslt = {ok, SRslt},
                ?Debug("statement ~p stored in ~p", [StmtRef, [S|| {S,_} <- Stmts]]),
                gen_server:reply(From, Rslt),
                {noreply, State#state{stmts=Stmts}};
            Term ->
                ?Debug("Async __RX__ ~p For ~p", [Term, From]),
                gen_server:reply(From, Term),
                {noreply, State}
    end;

% unhandled
handle_info(Info, State) ->
    ?Error([session, self()], "unknown info ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{conn_param={Type, Opts},stmts=Stmts}) ->
    _ = [StmtFsm:stop() || #stmt{fsm=StmtFsm} <- Stmts],
    ?Debug("stopped ~p config ~p for ~p", [self(), {Type, Opts}, Reason]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.


%
% private functions
%

-spec get_seco(binary(), binary() | atom(), {atom(), term()}, binary(), undefined | binary()) -> undefined | integer().
get_seco(_, _, {local, _}, _, _) -> undefined;
get_seco(User, SessionId, Connect, Pswd, undefined) ->
    ?Debug("New Password is undefined"),
    S = authenticate_user(User, SessionId, Connect, Pswd),
    erlimem_cmds:exec(undefined, {login,S}, Connect),
    {undefined, S} = erlimem_cmds:recv_sync(Connect, <<>>, 0),
    ?Debug("logged in ~p", [{User, S}]),
    case Connect of
        {gen_tcp,Sck} -> inet:setopts(Sck,[{active,once}]);
        {ssl,Sck}     -> ssl:setopts(Sck,[{active,once}]);
        _ -> ok
    end,
    S;
get_seco(User, SessionId, Connect, Pswd, NewPswd) when is_binary(NewPswd) ->
    S = authenticate_user(User, SessionId, Connect, Pswd),
    NewSeco = change_password(S, Connect, Pswd, NewPswd),
    ?Debug("password changed ~p", [{User, NewSeco}]),
    case Connect of
        {gen_tcp,Sck} -> inet:setopts(Sck,[{active,once}]);
        {ssl,Sck}     -> ssl:setopts(Sck,[{active,once}]);
        _ -> ok
    end,
    NewSeco.

-spec authenticate_user(binary(), binary() | atom(), {atom(), term()}, binary()) -> integer().
authenticate_user(User, SessionId, Connect, Pswd) ->
    erlimem_cmds:exec(undefined, {authenticate, undefined, SessionId, User, {pwdmd5, Pswd}}, Connect),
    {undefined, S} = erlimem_cmds:recv_sync(Connect, <<>>, 0),
    ?Debug("authenticated ~p -> ~p", [User, S]),
    S.

-spec change_password(integer(), {atom(), term()}, binary(), binary()) -> integer().
change_password(S, Connect, Pswd, NewPswd) ->
    ?Debug("Changing password, params: ~p", [{S, Connect, Pswd, NewPswd}]),
    erlimem_cmds:exec(undefined, {change_credentials, S, {pwdmd5, Pswd}, {pwdmd5, NewPswd}}, Connect),
    {undefined, NewSeco} = erlimem_cmds:recv_sync(Connect, <<>>, 0),
    case NewSeco of
        S -> ok;
        _ -> logout(S, Connect)
    end,
    NewSeco.

-spec connect(atom(), tuple()) -> {ok, {atom(), term()}, term()} | {error, term()}.
connect(tcp, {IpAddr, Port, Schema}) -> connect(tcp, {IpAddr, Port, Schema, []});
connect(tcp, {IpAddr, Port, Schema, Opts}) ->
    {TcpMod, InetMod} = case lists:member(ssl, Opts) of true -> {ssl, ssl}; _ -> {gen_tcp, inet} end,
    {ok, Ip} = inet:getaddr(IpAddr, inet),
    ?Debug("connecting to ~p:~p ~p", [Ip, Port, Opts]),
    case TcpMod:connect(Ip, Port, []) of
        {ok, Socket} ->
            InetMod:setopts(Socket, [{active, false}, binary, {packet, 0}, {nodelay, true}]),
            {ok, {case lists:member(ssl, Opts) of true -> ssl; _ -> gen_tcp end, Socket}, Schema};
        {error, _} = Error -> Error
    end;
connect(rpc, {Node, Schema}) when Node == node() -> connect(local_sec, {Schema});
connect(rpc, {Node, Schema}) when is_atom(Node)  -> {ok, {rpc, Node}, Schema};
connect(local_sec, {Schema})                     -> {ok, {local_sec, undefined}, Schema};
connect(local, {Schema})                         -> {ok, {local, undefined}, Schema}.

-spec logout(integer(), {atom(), term()}) -> ok | {error, atom()}.
logout(S, Connect) ->
    erlimem_cmds:exec(undefined, {logout, S}, Connect).

% tcp helpers
-spec split_packages(integer(), binary()) -> {integer(), binary(), [binary()]}.
split_packages(0, <<>>) -> {0, <<>>, []};
split_packages(Len, Payload) when Len > byte_size(Payload) ->
    ?Debug(" [INCOMPLETE] ~p received ~p of ~p bytes buffering...", [self(), byte_size(Payload), Len]),
    {Len, Payload, []};
split_packages(Len, Payload) when Len =:= byte_size(Payload) ->
    {0, <<>>, [Payload]};
split_packages(Len, Payload) ->
    <<Command:Len/binary, Rest/binary>> = Payload,
    if
        byte_size(Rest) < 4 ->
            ?Debug(" [INCOMPLETE] ~p received header ~p of 4 bytes buffering...", [self(), byte_size(Rest)]),
            {0, Rest, [Command]};
        true ->
            <<NewLen:32, NewPayload/binary>> = Rest,
            {ResultLen, ResultPayload, Commands} = split_packages(NewLen, NewPayload),
            {ResultLen, ResultPayload, [Command|Commands]}
    end.

-spec process_commands([binary()], #state{}) -> #state{}.
process_commands([], State) -> State;
process_commands([<<>>|Rest], State) -> process_commands(Rest, State);
process_commands([Command|Rest], State) ->
    ?Debug("RX ~p = bytes of term", [byte_size(Command)]),
    NewState = case (catch binary_to_term(Command)) of
        {'EXIT', _Reason} ->
            ?Error(" [MALFORMED] RX ~p byte of term, ignoring command ~p ...", [byte_size(Command)]),
            State;
        {From, {error, Exception}} ->
            ?Error("to ~p throw~n~p~n", [From, Exception]),
            gen_server:reply(From,  {error, Exception}),
            State;
        {From, Term} ->
            case Term of
                {ok, #stmtResult{}} = Resp ->
                    ?Debug("TCP async __RX__ ~p For ~p", [Term, From]),
                    {noreply, ResultState} = handle_info({From,Resp}, State),
                    ResultState;
                {StmtRef,{Rows,Completed}} when is_pid(StmtRef) ->
                    ?Debug("TCP async __RX__ ~p For ~p", [Term, From]),
                    {noreply, ResultState} = handle_info({From,{StmtRef,{Rows,Completed}}}, State),
                    ResultState;
                _ ->
                    ?Debug("TCP async __RX__ ~p For ~p", [Term, From]),
                    gen_server:reply(From, Term),
                    State
            end
    end,
    process_commands(Rest, NewState).
