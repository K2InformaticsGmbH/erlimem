-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").
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
        result,
        maxrows,
        completed,
        cmdstr
    }).

%% session APIs
-export([close/1
        , exec/2
        , exec/3
        , exec/4
        , run_cmd/3
        , get_stmts/1
		]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% statement APIs
-export([
    gui_req/2
]).

close({?MODULE, Pid})          -> gen_server:call(Pid, stop);
close({?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {close_statement, StmtRef}).

exec(StmtStr,                                    Ctx) -> exec(StmtStr, 0, Ctx).
exec(StmtStr, BufferSize,                        Ctx) -> run_cmd(exec, [StmtStr, BufferSize], Ctx).
exec(StmtStr, BufferSize, Fun,                   Ctx) -> run_cmd(exec, [StmtStr, BufferSize, Fun], Ctx).
run_cmd(Cmd, Args, {?MODULE, Pid}) when is_list(Args) -> call(Pid, [Cmd|Args]).
%row_with_key(RowId,          {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {row_with_key, StmtRef, RowId}).
gui_req(Button,              {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {button, StmtRef, Button}).

get_stmts(PidStr) ->
    gen_server:call(list_to_pid(PidStr), get_stmts).

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
%% handle_call({close_statement, StmtRef}, From, #state{connection=Connection,seco=SeCo,stmts=Stmts}=State) ->
%%     case lists:keytake(StmtRef, 1, Stmts) of
%%         {value, {StmtRef, #drvstmt{buf=Buf}}, NewStmts} ->
%%             ?Debug("delete ~p stmts ~p", [StmtRef, [S|| {S,_} <- NewStmts]]),
%%             erlimem_buf:delete(Buf),
%%             erlimem_cmds:exec({close, SeCo, StmtRef}, Connection);
%%         false -> NewStmts = Stmts
%%     end,
%%     {noreply,State#state{pending=From, stmts=NewStmts}};
%% 
%% handle_call({update_row, RowId, ColumId, Val, StmtRef}, From, #state{stmts=Stmts} = State) ->
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf} = Stmt,
%%     Row = erlimem_buf:get_row_at(Buf, RowId),
%%     if Row =:= [] -> {reply,{error, "Row not found"},State};
%%     true ->
%%         [R|_] = Row,
%%         NewRow = lists:sublist(R,ColumId) ++ [Val] ++ lists:nthtail(ColumId+1,R),
%%         ?Debug("update_row from ~p to ~p", [R, NewRow]),
%%         handle_call({modify_rows, upd, [NewRow], StmtRef}, From, State)
%%     end;
%% handle_call({delete_row, RowId, StmtRef}, From, #state{stmts=Stmts} = State) ->
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf} = Stmt,
%%     Row = erlimem_buf:get_row_at(Buf, RowId),
%%     handle_call({modify_rows, del, [Row], StmtRef}, From, State);
%% handle_call({insert_row, Clm, Val, StmtRef}, From, #state{stmts=Stmts} = State) ->
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf, result={columns, Clms}} = Stmt,
%%     ColNameList = [binary_to_list(C#stmtCol.alias)||C<-Clms],
%%     ?Debug("insert_row clm ~p in ~p",[Clm, ColNameList]),
%%     ColumId = string:str(ColNameList,[Clm]),
%%     Row = lists:duplicate(length(Clms), ""),
%%     NewRow = lists:sublist(Row,ColumId-1) ++ [Val] ++ lists:nthtail(ColumId,Row),
%%     {reply,ok,NewState} = handle_call({modify_rows, ins, [NewRow], StmtRef}, From, State),
%%     {ok,Count} = erlimem_buf:get_buffer_max(Buf),
%%     {reply,Count,NewState};
%% 
%% handle_call({clear_buf, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf,completed=Completed} = Stmt,
%%     NewBuf = if Completed =:= true -> erlimem_buf:clear(Buf); true -> Buf end,
%%     NewStmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
%%     ?Debug("replace ~p stmts ~p", [StmtRef, [S|| {S,_} <- NewStmts]]),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply,ok,State#state{idle_timer=NewTimer,stmts=NewStmts}};
%% handle_call({get_buffer_max, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf,completed=C} = Stmt,
%%     {ok, Count} = erlimem_buf:get_buffer_max(Buf),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     Completed = if (C =:= true) orelse (C =:= false) -> C; true -> false end,
%%     {reply, {ok,Completed,Count}, State#state{idle_timer=NewTimer}};
%% handle_call({rows_from, StmtRef, RowId}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, #drvstmt{ buf = Buf
%%                 , maxrows = MaxRows
%%                 , fetchonfly = FReq
%%                 , fetchopts = Opts
%%                 , cmdstr = Sql} = Stmt
%%     } = lists:keyfind(StmtRef, 1, Stmts),
%%     {{Rows,Status,CatchSize}, NewBuf} = erlimem_buf:get_rows_from(Buf, RowId, MaxRows),
%%     NewFReq = if
%%         (FReq < ?MAX_PREFETCH_ON_FLIGHT)  -> FReq + 1;
%%         (FReq == ?MAX_PREFETCH_ON_FLIGHT) -> ?MAX_PREFETCH_ON_FLIGHT;
%%         true                              -> FReq
%%     end,
%%     ?Debug("fetch on flight ~p", [NewFReq]),
%%     if
%%         (FReq =:= 0) andalso (NewFReq > 0) ->
%%             ?Debug("-> prefetching for ~p ...", [Sql]),
%%             gen_server:cast(self(), {read_block_async, Opts, StmtRef});
%%         true -> ok
%%     end,
%%     NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
%%     ?Debug("add ~p stmts ~p", [StmtRef, [S|| {S,_} <- NewStmts]]),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     ?Info("rows_from rows ~p buf status ~p buf size ~p from ~p", [length(Rows),Status,CatchSize,RowId]),
%%     {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
%% handle_call({prev_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf, maxrows=MaxRows} = Stmt,
%%     {{Rows,Status,CatchSize}, NewBuf} = erlimem_buf:get_prev_rows(Buf, MaxRows),
%%     NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
%%     ?Debug("add ~p stmts ~p", [StmtRef, [S|| {S,_} <- NewStmts]]),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     ?Info("prev_rows rows ~p buf status ~p buf size ~p", [length(Rows),Status,CatchSize]),
%%     {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
%% handle_call({next_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_,
%%         #drvstmt{ buf = Buf
%%                 , maxrows = MaxRows
%%                 , fetchonfly = FReq
%%                 , fetchopts = Opts} = Stmt
%%     } = lists:keyfind(StmtRef, 1, Stmts),
%%     {{Rows,Status,CatchSize}, NewBuf} = erlimem_buf:get_next_rows(Buf, MaxRows),
%%     NewFReq = if
%%         (FReq < ?MAX_PREFETCH_ON_FLIGHT)  -> FReq + 1;
%%         (FReq == ?MAX_PREFETCH_ON_FLIGHT) -> ?MAX_PREFETCH_ON_FLIGHT;
%%         true                              -> FReq
%%     end,
%%     ?Debug("fetch on flight ~p", [NewFReq]),
%%     if
%%         (FReq =:= 0) andalso (NewFReq > 0) ->
%%             gen_server:cast(self(), {read_block_async, Opts, StmtRef});
%%         true -> ok
%%     end,
%%     NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf, fetchonfly = NewFReq}}),
%%     ?Debug("add/replace ~p stmts ~p", [StmtRef, [S|| {S,_} <- NewStmts]]),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     ?Info("next_rows rows ~p buf status ~p buf size ~p", [length(Rows),Status,CatchSize]),
%%     {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
%% handle_call({modify_rows, Op, Rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
%%     #drvstmt{buf=Buf} = Stmt,
%%     erlimem_buf:modify_rows(Buf, Op, Rows),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply,ok,State#state{idle_timer=NewTimer}};
%% handle_call({row_with_key, StmtRef, RowId}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
%%     [Row] = erlimem_buf:row_with_key(Buf, RowId),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply,Row,State#state{idle_timer=NewTimer}};
handle_call({button, StmtRef, Button}, From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    ?Debug("~p in statements ~p", [StmtRef, Stmts]),
    {_, #drvstmt{fsm=StmtFsm}} = lists:keyfind(StmtRef, 1, Stmts),     
    StmtFsm:gui_req("button", Button,
       fun(#gres{} = Gres) ->
           ?Info("resp for gui ~p", [Gres]),
           gen_server:reply(From, Gres)
       end),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {noreply,State#state{idle_timer=NewTimer}};

%% handle_call({fetch_close, StmtRef}, _From, #state{connection=Connection,seco=SeCo,idle_timer=Timer} = State) ->
%%     ?Info("fetch_close ~p", [StmtRef]),
%%     erlang:cancel_timer(Timer),
%%     erlimem_cmds:exec({fetch_close, SeCo, StmtRef}, Connection),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply,ok,State#state{idle_timer=NewTimer}};
%% handle_call({fetch_recs_async, Opts, StmtRef}, _From, #state{connection=Connection,seco=SeCo,idle_timer=Timer} = State) ->
%%     ?Info("fetch_recs_async ~p", [StmtRef]),
%%     erlang:cancel_timer(Timer),
%%     R = erlimem_cmds:exec({fetch_recs_async, SeCo, Opts, StmtRef}, Connection),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply,R,State#state{idle_timer=NewTimer}};
%% 
%% handle_call({prepare_update, StmtRef}, From, #state{connection=Connection,seco=SeCo,idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
%%     Rows = erlimem_buf:get_modified_rows(Buf),
%%     ?Debug([session, self()], "modified rows ~p", [Rows]),
%%     erlimem_cmds:exec({update_cursor_prepare, SeCo, StmtRef, Rows}, Connection),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {noreply, State#state{idle_timer=NewTimer,pending=From}};
%% handle_call({execute_update, StmtRef}, From, #state{connection=Connection,seco=SeCo,idle_timer=Timer} = State) ->
%%     erlang:cancel_timer(Timer),
%%     erlimem_cmds:exec({update_cursor_execute, SeCo, StmtRef, optimistic}, Connection),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {noreply, State#state{idle_timer=NewTimer,pending=From}};
%% handle_call({update_keys, StmtRef, Updates}, From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
%%     erlang:cancel_timer(Timer),
%%     {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
%%     ok = erlimem_buf:update_keys(Buf, Updates),
%%     NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
%%     {reply, ok, State#state{idle_timer=NewTimer,pending=From}};

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
                % TODO setup monitor with _StmtFsmPid
                erlang:monitor(process, StmtFsmPid),
                NStmts = lists:keystore(StmtRef, 1, Stmts,
                            {StmtRef, #drvstmt{ result   = {columns, Clms}
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
                    ?Info("~p __RX__ received rows ~p status ~p", [StmtRef, length(Rows), Completed]),
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
    ?Info("FSM ~p is died with reason ~p for stmt ~p remaining ~p", [StmtFsmPid, Reason, StmtRef, NewStmts]),
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
    ?Debug([session, self()], "~p close on timeout", [{?MODULE,?LINE}]),
    {stop,normal,State};
handle_info(Info, State) ->
    ?Error([session, self()], "unknown info ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{conn_param={Type, Opts},idle_timer=Timer,stmts=_Stmts}) ->
    erlang:cancel_timer(Timer),
    %% _ = [erlimem_buf:delete(Buf) || #drvstmt{buf=Buf} <- Stmts], % TODO for FSM
    ?Debug([session, self()], "~p stopped ~p from ~p", [{?MODULE,?LINE}, self(), {Type, Opts}]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.
