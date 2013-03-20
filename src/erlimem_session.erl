-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").
-include("gres.hrl").
-include_lib("imem/include/imem_sql.hrl").

-record(state, {
    status=closed,
    stmts = [],
    connection = {type, handle},
    conn_param,
    idle_timer,
    schema,
    seco = undefined,
    event_pids=[],
    pending,
    buf = <<>>,
    maxrows
}).

-record(drvstmt, {
        fsm,
        columns,
        maxrows,
        completed,
        cmdstr
    }).

%% session APIs
-export([ close/1
        , exec/2
        , exec/3
        , exec/4
        , run_cmd/3
        , get_stmts/1
		]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% statement APIs
-export([ get_columns/1
        , gui_req/3
        , row_with_key/2
        ]).

close({?MODULE, Pid})          -> gen_server:call(Pid, stop);
close({?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {close_statement, StmtRef}).

exec(StmtStr,                                    Ctx) -> exec(StmtStr, 0, Ctx).
exec(StmtStr, BufferSize,                        Ctx) -> run_cmd(exec, [StmtStr, BufferSize], Ctx).
exec(StmtStr, BufferSize, Fun,                   Ctx) -> run_cmd(exec, [StmtStr, BufferSize, Fun], Ctx).
run_cmd(Cmd, Args, {?MODULE, Pid}) when is_list(Args) -> call(Pid, [Cmd|Args]).
row_with_key(RowId,          {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {row_with_key, StmtRef, RowId}).
gui_req(Cmd, Param,          {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {gui_req, StmtRef, Cmd, Param}).
get_columns(                 {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {columns, StmtRef}).

get_stmts(PidStr) -> gen_server:call(list_to_pid(PidStr), get_stmts).

call(Pid, Msg) ->
    ?Debug("call ~p ~p", [Pid, Msg]),
    gen_server:call(Pid, Msg, ?IMEM_TIMEOUT).

init([Type, Opts, {User, Pswd}]) when is_binary(User), is_binary(Pswd) ->
    ?Debug("connecting with ~p cred ~p", [{Type, Opts}, {User, Pswd}]),
    PswdMD5 = case io_lib:printable_unicode_list(binary_to_list(Pswd)) of
        true -> erlang:md5(Pswd);
        false -> Pswd
    end,
    case connect(Type, Opts) of
        {ok, Connect, Schema} ->
            try
                SeCo =  case Connect of
                    {local, _} -> undefined;
                    _ ->
                        erlimem_cmds:exec({authenticate, undefined, adminSessionId, User, {pwdmd5, PswdMD5}}, Connect),
                        {resp, S} = erlimem_cmds:recv_sync(Connect, <<>>),
                        ?Info("authenticated ~p -> ~p", [{User, PswdMD5}, S]),
                        erlimem_cmds:exec({login,S}, Connect),
                        {resp, S} = erlimem_cmds:recv_sync(Connect, <<>>),
                        ?Info("logged in ~p", [{User, S}]),
                        case Connect of
                            {tcp,Sck} -> inet:setopts(Sck,[{active,once}]);
                            _ -> ok
                        end,
                        S
                end,
                ?Info("started ~p connected to ~p", [self(), {Type, Opts}]),
                Timer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
                {ok, #state{connection=Connect, schema=Schema, conn_param={Type, Opts}, idle_timer=Timer, seco=SeCo}}
            catch
            _Class:{Result,ST} ->
                ?Error("erlimem connect error ~p", [Result]),
                ?Debug("erlimem connect error stackstrace ~p", [ST]),
                case Connect of
                    {tcp, Sock} -> gen_tcp:close(Sock);
                    _ -> ok
                end,
                {stop, Result}
            end;
        {error, Reason} ->
            ?Error("erlimem connect error ~p", [Reason]),
            {stop, Reason}
    end.

connect(tcp, {IpAddr, Port, Schema}) ->
    {ok, Ip} = inet:getaddr(IpAddr, inet),
    ?Info("connecting to ~p:~p", [Ip, Port]),
    case gen_tcp:connect(Ip, Port, []) of
        {ok, Socket} ->
            inet:setopts(Socket, [{active, false}, binary, {packet, 0}, {nodelay, true}]),
            {ok, {tcp, Socket}, Schema};
        {error, _} = Error -> Error
    end;
connect(rpc, {Node, Schema}) when Node == node() -> connect(local_sec, {Schema});
connect(rpc, {Node, Schema}) when is_atom(Node)  -> {ok, {rpc, Node}, Schema};
connect(local_sec, {Schema})                     -> {ok, {local_sec, undefined}, Schema};
connect(local, {Schema})                         -> {ok, {local, undefined}, Schema}.

handle_call(get_stmts, _From, #state{stmts=Stmts} = State) ->    
    {reply,[S|| {S,_} <- Stmts],State};
handle_call(stop, _From, State) ->
    {stop,normal, ok, State};
handle_call({gui_req, StmtRef, Cmd, Param}, From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    ?Debug("~p in statements ~p", [StmtRef, Stmts]),
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #drvstmt{fsm=StmtFsm}} ->
            StmtFsm:gui_req(Cmd, Param,
                fun(#gres{} = Gres) ->
                    ?Debug("resp for gui ~p", [Gres]),
                    gen_server:reply(From, Gres)
                end),
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {noreply,State#state{idle_timer=NewTimer}};
        false ->
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {reply,#gres{},State#state{idle_timer=NewTimer}}
    end;
handle_call({columns, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #drvstmt{columns = Columns}} ->
            ?Debug("~p columns ~p", [StmtRef, Columns]),
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {reply,Columns,State#state{idle_timer=NewTimer}};
        false ->
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {reply,[],State#state{idle_timer=NewTimer}}
    end;
handle_call({row_with_key, StmtRef, RowId}, From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    ?Debug("~p in statements ~p", [StmtRef, Stmts]),
    {_, #drvstmt{fsm=StmtFsm}} = lists:keyfind(StmtRef, 1, Stmts),     
    StmtFsm:row_with_key(RowId,
        fun(Row) ->
            ?Debug("row_with_key ~p", [Row]),
            gen_server:reply(From, Row)
        end),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {noreply,State#state{idle_timer=NewTimer}};

handle_call(Msg, {From, _} = Frm, #state{connection=Connection
                                        ,idle_timer=Timer
                                        ,schema=Schema
                                        ,seco=SeCo
                                        ,event_pids=EvtPids} = State) ->
    ?Debug("call ~p", [Msg]),
    erlang:cancel_timer(Timer),
    [Cmd|Rest] = Msg,
    NewMsg = case Cmd of
        exec ->
            NewEvtPids = EvtPids,
            [_,MaxRows|_] = Rest,
            list_to_tuple([Cmd,SeCo|Rest] ++ [Schema]);
        subscribe ->
            MaxRows = 0,
            [Evt|_] = Rest,
            NewEvtPids = lists:keystore(Evt, 1, EvtPids, {Evt, From}),
            list_to_tuple([Cmd,SeCo|Rest]);
        _ ->
            NewEvtPids = EvtPids,
            MaxRows = 0,
            list_to_tuple([Cmd,SeCo|Rest])
    end,
    case (catch erlimem_cmds:exec(NewMsg, Connection)) of
        {{error, E}, ST} ->
            ?Error("cmd ~p error ~p", [Cmd, E]),
            ?Debug("~p", [ST]),
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {reply, E, State#state{idle_timer=NewTimer,event_pids=NewEvtPids,pending=Frm,maxrows=MaxRows}};
        _Result ->
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {noreply,State#state{idle_timer=NewTimer,event_pids=NewEvtPids,pending=Frm,maxrows=MaxRows}}
    end.

handle_cast(Request, State) ->
    ?Error([session, self()], "unknown cast ~p", [Request]),
    {noreply, State}.

%
% local / rpc / tcp fallback
%
handle_info({resp,Resp}, #state{pending=Form, stmts=Stmts,maxrows=MaxRows}=State) ->
    case Resp of
            {error, Exception} ->
                ?Error("throw ~p", [Exception]),
                throw(Exception);
            {ok, #stmtResult{stmtCols = Clms, rowFun = Fun, stmtRef = StmtRef} = SRslt} when is_function(Fun) ->
                ?Debug("RX ~p", [SRslt]),
                {_,StmtFsmPid} = StmtFsm = erlimem_fsm:start_link(SRslt, "what is it?", MaxRows),
                % monitoring StmtFsmPid
                erlang:monitor(process, StmtFsmPid),
                NStmts = lists:keystore(StmtRef, 1, Stmts,
                            {StmtRef, #drvstmt{ columns  = Clms
                                              , fsm      = StmtFsm
                                              , maxrows  = MaxRows}
                                              %, cmdstr   = Sql}
                            }),
                Rslt = {ok, Clms, {?MODULE, StmtRef, self()}},
                ?Debug("statement ~p stored in ~p", [StmtRef, [S|| {S,_} <- NStmts]]),
                gen_server:reply(Form, Rslt),
                {noreply, State#state{stmts=NStmts}};
            Term ->
                ?Debug("Async __RX__ ~p For ~p", [Term, Form]),
                gen_server:reply(Form, Term),
                {noreply, State}
    end;
handle_info({StmtRef,{Rows,Completed}}, #state{stmts=Stmts}=State) when is_pid(StmtRef) ->
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #drvstmt{fsm=StmtFsm}} ->
            case {Rows,Completed} of
                {error, Result} ->
                    ?Error([session, self()], "async_resp ~p", [Result]),
                    {noreply, State};
                {Rows, Completed} when Completed =:= true;
                                       Completed =:= false;
                                       Completed =:= tail ->
                    StmtFsm:rows({Rows,Completed}),
                    ?Debug("~p __RX__ received rows ~p status ~p", [StmtRef, length(Rows), Completed]),
                    {noreply, State};
                Unknown ->
                    ?Error([session, self()], "async_resp unknown resp ~p", [Unknown]),
                    {noreply, State}
            end;
        false ->
            ?Error("statement ~p not found in ~p", [StmtRef, [S|| {S,_} <- Stmts]]),
            {noreply, State}
    end;

%
% tcp
%
handle_info({tcp,S,Pkt}, #state{buf=Buf, pending=Form}=State) ->
    NewBin = << Buf/binary, Pkt/binary >>,
    NewState =
    case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Debug("RX ~p byte of term, waiting...", [byte_size(Pkt)]),
                State#state{buf=NewBin};
            {error, Exception} ->
                ?Error("throw ~p", [Exception]),
                gen_server:reply(Form,  {error, Exception}),
                State;
            Term ->
                NState = case Term of
                    {ok, #stmtResult{}} = Resp ->
                        {_, NS} = handle_info({resp,Resp}, State),
                        NS;
                    {StmtRef,{Rows,Completed}} when is_pid(StmtRef) ->
                        {_, NS} = handle_info({StmtRef,{Rows,Completed}}, State),
                        NS;
                    _ ->
                        ?Debug("TCP async __RX__ ~p For ~p", [Term, Form]),
                        gen_server:reply(Form, Term),
                        State
                end,
                TSize = byte_size(term_to_binary(Term)),
                RestSize = byte_size(NewBin)-TSize,
                {noreply, NwState} = handle_info({tcp,S,binary_part(NewBin, {TSize, RestSize})}, NState#state{buf= <<>>}),
                NwState
    end,
    inet:setopts(S,[{active,once}]),
    {noreply, NewState};

%
% FSM Monitor events
%
handle_info({'DOWN', Ref, process, StmtFsmPid, Reason}, #state{stmts=Stmts}=State) ->
    [StmtRef|_] = [SR || {SR, DS} <- Stmts, DS#drvstmt.fsm =:= {erlimem_fsm, StmtFsmPid}],
    NewStmts = lists:keydelete(StmtRef, 1, Stmts),
    true = demonitor(Ref, [flush]),
    ?Info("FSM ~p is died with reason ~p for stmt ~p remaining ~p", [StmtFsmPid, Reason, StmtRef, [S || {S,_} <- NewStmts]]),
    {noreply, State#state{stmts=NewStmts}};

%
% MNESIA Events
%
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

handle_info(timeout, State) ->
    ?Debug("~p close on timeout", [self()]),
    {stop,normal,State};
handle_info({tcp_closed,Sock}, State) ->
    ?Debug("close on tcp_close ~p", [self(), Sock]),
    {stop,normal,State};
handle_info(Info, State) ->
    ?Error([session, self()], "unknown info ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{conn_param={Type, Opts},idle_timer=Timer,stmts=Stmts}) ->
    erlang:cancel_timer(Timer),
    _ = [StmfFsm:gui_req("close") || #drvstmt{fsm=StmfFsm} <- Stmts],
    ?Debug("stopped ~p config ~p for ~p", [self(), {Type, Opts}, Reason]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.
