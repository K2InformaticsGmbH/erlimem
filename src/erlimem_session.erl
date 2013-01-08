-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").
-include_lib("imem/include/imem_meta.hrl").

-record(state, {
    status=closed,
    port,
    pool,
    session_id,
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
        buf,
        result,
        maxrows,
        completed = false,
        tail = false
    }).

%% session APIs
-export([close/1
        , exec/2
        , exec/3
        , exec/4
        , read_block/2
        , run_cmd/3
		]).

% statement APIs
-export([start_async_read/1
        , start_async_read/2
        , get_buffer_max/1
        , rows_from/2
        , prev_rows/1
        , next_rows/1
        , update_rows/2
        , update_row/4
        , delete_row/2
        , insert_row/3
        , delete_rows/2
        , insert_rows/2
        , commit_modified/1
        , row_with_key/2
        , fetch_close/1
		]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).


close({?MODULE, Pid})          -> gen_server:call(Pid, stop);
close({?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {close_statement, StmtRef}).

exec(StmtStr,                                    Ctx) -> exec(StmtStr, 0, Ctx).
exec(StmtStr, BufferSize,                        Ctx) -> run_cmd(exec, [StmtStr, BufferSize], Ctx).
exec(StmtStr, BufferSize, Fun,                   Ctx) -> run_cmd(exec, [StmtStr, BufferSize, Fun], Ctx).
read_block(StmtRef,                              Ctx) -> run_cmd(read_block, [StmtRef], Ctx).
run_cmd(Cmd, Args, {?MODULE, Pid}) when is_list(Args) -> call(Pid, [Cmd|Args]).
get_buffer_max(              {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {get_buffer_max, StmtRef}).
rows_from(RowId,             {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {rows_from, StmtRef, RowId}).
prev_rows(                   {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {prev_rows, StmtRef}).
next_rows(                   {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {next_rows, StmtRef}).
update_row(RId, CId, Val,    {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {update_row, RId, CId, Val, StmtRef}).
delete_row(RId,              {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {delete_row, RId, StmtRef}).
insert_row(CId, Val,         {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {insert_row, CId, Val, StmtRef}).
update_rows(Rows,            {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {modify_rows, upd, Rows, StmtRef}).
delete_rows(Rows,            {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {modify_rows, del, Rows, StmtRef}).
insert_rows(Rows,            {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {modify_rows, ins, Rows, StmtRef}).
commit_modified(             {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {commit_modified, StmtRef}).
row_with_key(RowId,          {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {row_with_key, StmtRef, RowId}).
fetch_close(                 {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {fetch_close, StmtRef}).
start_async_read(                                Ctx) -> start_async_read([], Ctx).
start_async_read(Opts,       {?MODULE, StmtRef, Pid}) ->
    ok = gen_server:call(Pid, {clear_buf, StmtRef}),
    gen_server:cast(Pid, {read_block_async, Opts, StmtRef}).

call(Pid, Msg) ->
    ?Debug("~p call ~p ~p", [?MODULE, Pid, Msg]),
    gen_server:call(Pid, Msg, ?IMEM_TIMEOUT).

init([Type, Opts, {User, Password}]) when is_binary(User), is_binary(Password) ->
    ?Debug("~p connecting with ~p cred ~p", [?MODULE, {Type, Opts}, {User, Password}]),
    case connect(Type, Opts) of
        {ok, Connect, Schema} ->
            try
                SeCo =  case Connect of
                    {local, _} -> undefined;
                    _ ->
                        erlimem_cmds:exec({authenticate, undefined, adminSessionId, User, {pwdmd5, Password}}, Connect),
                        {resp, S} = erlimem_cmds:recv_sync(Connect, <<>>),
                        ?Info("authenticated ~p -> ~p", [{User, Password}, S]),
                        erlimem_cmds:exec({login,S}, Connect),
                        {resp, S} = erlimem_cmds:recv_sync(Connect, <<>>),
                        ?Info("logged in ~p", [{User, S}]),
                        case Connect of
                            {tcp,Sck} -> inet:setopts(Sck,[{active,once}]);
                            _ -> ok
                        end,
                        S
                end,
                ?Info("~p started ~p connected to ~p", [?MODULE, self(), {Type, Opts}]),
                Timer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
                {ok, #state{connection=Connect, schema=Schema, conn_param={Type, Opts}, idle_timer=Timer, seco=SeCo}}
            catch
            _Class:{Result,ST} ->
                ?Error("~p erlimem connect error ~p", [?MODULE, Result]),
                ?Debug("~p erlimem connect error stackstrace ~p", [?MODULE, ST]),
                case Connect of
                    {tcp, Sock} -> gen_tcp:close(Sock);
                    _ -> ok
                end,
                {stop, Result}
            end;
        {error, Reason} ->
            ?Error("~p erlimem connect error ~p", [?MODULE, Reason]),
            {stop, Reason}
    end.

connect(tcp, {IpAddr, Port, Schema}) ->
    {ok, Ip} = inet:getaddr(IpAddr, inet),
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

handle_call(stop, _From, #state{stmts=Stmts}=State) ->
    _ = [erlimem_buf:delete(Buf) || #drvstmt{buf=Buf} <- Stmts],
    {stop,normal,State};
handle_call({close_statement, StmtRef}, From, #state{connection=Connection,seco=SeCo,stmts=Stmts}=State) ->
    case lists:keytake(StmtRef, 1, Stmts) of
        {value, {StmtRef, #drvstmt{buf=Buf}}, NewStmts} ->
            erlimem_buf:delete(Buf),
            erlimem_cmds:exec({close, SeCo, StmtRef}, Connection);
        false -> NewStmts = Stmts
    end,
    {noreply,State#state{pending=From, stmts=NewStmts}};

handle_call({update_row, RowId, ColumId, Val, StmtRef}, From, #state{stmts=Stmts} = State) ->
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf} = Stmt,
    {{[Row],_,_}, _} = erlimem_buf:get_rows_from(Buf, RowId, 1),
    NewRow = lists:sublist(Row,ColumId) ++ [Val] ++ lists:nthtail(ColumId+1,Row),
    handle_call({modify_rows, upd, [NewRow], StmtRef}, From, State);
handle_call({delete_row, RowId, StmtRef}, From, #state{stmts=Stmts} = State) ->
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf} = Stmt,
    {{[Row],_,_}, _} = erlimem_buf:get_rows_from(Buf, RowId, 1),
    handle_call({modify_rows, del, [Row], StmtRef}, From, State);
handle_call({insert_row, Clm, Val, StmtRef}, From, #state{stmts=Stmts} = State) ->
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf, result={columns, Clms}} = Stmt,
    ColNameList = [atom_to_list(C#ddColMap.name)||C<-Clms],
    ColumId = string:str(ColNameList,[Clm]),
    Row = [C#ddColMap.default||C<-Clms],
    NewRow = lists:sublist(Row,ColumId-1) ++ [Val] ++ lists:nthtail(ColumId,Row),
    {reply,ok,NewState} = handle_call({modify_rows, ins, [NewRow], StmtRef}, From, State),
    {_,_,Count} = erlimem_buf:get_buffer_max(Buf),
    {reply,Count,NewState};

handle_call({clear_buf, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf} = Stmt,
    NewBuf = erlimem_buf:clear(Buf),
    NewStmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,ok,State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({get_buffer_max, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf} = Stmt,
    Count = erlimem_buf:get_buffer_max(Buf),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Count,State#state{idle_timer=NewTimer}};
handle_call({rows_from, StmtRef, RowId}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, #drvstmt{completed = Completed, tail = Tail} = Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf, maxrows=MaxRows} = Stmt,
    {Rows, NewBuf} = erlimem_buf:get_rows_from(Buf, RowId, MaxRows),
    if (Completed =:= false) andalso (Tail =:= false) ->
        io:format(user, "prefetch...~n", []),
        gen_server:cast(self(), {read_block_async, [], StmtRef});
        true -> ok
    end,
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Rows,State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({prev_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf, maxrows=MaxRows} = Stmt,
    {Rows, NewBuf} = erlimem_buf:get_prev_rows(Buf, MaxRows),
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Rows,State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({next_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, #drvstmt{completed = Completed, tail = Tail} = Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf, maxrows=MaxRows} = Stmt,
    {Rows, NewBuf} = erlimem_buf:get_next_rows(Buf, MaxRows),
    if (Completed =:= false) andalso (Tail =:= false) ->
        io:format(user, "prefetch...~n", []),
        gen_server:cast(self(), {read_block_async, [], StmtRef});
        true -> ok
    end,
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Rows,State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({modify_rows, Op, Rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf} = Stmt,
    erlimem_buf:modify_rows(Buf, Op, Rows),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,ok,State#state{idle_timer=NewTimer}};
handle_call({row_with_key, StmtRef, RowId}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
    [Row] = erlimem_buf:row_with_key(Buf, RowId),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Row,State#state{idle_timer=NewTimer}};
handle_call({fetch_close, StmtRef}, _From, #state{connection=Connection,seco=SeCo,idle_timer=Timer} = State) ->
io:format(user, "~p fetch_close~n", [StmtRef]),
    erlang:cancel_timer(Timer),
    erlimem_cmds:exec({fetch_close, SeCo, StmtRef}, Connection),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,ok,State#state{idle_timer=NewTimer}};
handle_call({commit_modified, StmtRef}, _From, #state{connection=Connection,seco=SeCo,idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
    Rows = erlimem_buf:get_modified_rows(Buf),
    ?Debug([session, self()], "~p modified rows ~p", [?MODULE, Rows]),
    Result =
    try
        erlimem_cmds:exec({update_cursor_prepare, SeCo, StmtRef, Rows}, Connection),
        erlimem_cmds:exec({update_cursor_execute, SeCo, StmtRef, optimistic}, Connection),
        erlimem_cmds:exec({fetch_close, SeCo, StmtRef}, Connection),
        Rows
    catch
        _Class:{Res,ST} ->
            ?Error([session, self()], "~p ~p", [?MODULE, Res]),
            ?Debug([session, self()], "~p error stack ~p", [?MODULE, ST]),
            {error, Res}
    end,
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,Result,State#state{idle_timer=NewTimer}};

handle_call(Msg, {From, _} = Frm, #state{connection=Connection
                                        ,idle_timer=Timer
                                        ,stmts=Stmts
                                        ,schema=Schema
                                        ,seco=SeCo
                                        ,event_pids=EvtPids} = State) ->
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
            ?Error("~p", [E]),
            ?Debug("~p", [ST]),
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {reply, E, State#state{idle_timer=NewTimer, event_pids=NewEvtPids,pending=Frm,maxrows=MaxRows}};
        Result ->
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {noreply,State#state{idle_timer=NewTimer, event_pids=NewEvtPids,pending=Frm,maxrows=MaxRows}}
    end.

handle_cast({read_block_async, Opts, StmtRef}, #state{stmts=Stmts, connection=Connection, seco=SeCo}=State) ->
io:format(user, "~p fetch_recs_async ~p~n", [StmtRef, Opts]),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    erlimem_cmds:exec({fetch_recs_async, SeCo, Opts, StmtRef}, Connection),    
    {noreply, State#state{stmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{tail=proplists:get_bool(tail_mode, Opts)}})}};
handle_cast(Request, State) ->
    ?Error([session, self()], "~p unknown cast ~p", [?MODULE, Request]),
    {noreply, State}.

% 
% local / rpc
%
handle_info({resp,Resp}, #state{pending=Form, stmts=Stmts,maxrows=MaxRows}=State) ->
    case Resp of
            {error, Exception} ->
                ?Error("~p throw ~p", [?MODULE, Exception]),
                throw(Exception);
            {ok, Clms, Fun, StmtRef} ->
                ?Debug("~p RX ~p", [?MODULE, {Clms, Fun, StmtRef}]),
                Rslt = {ok, Clms, {?MODULE, StmtRef, self()}},
                NStmts = lists:keystore(StmtRef, 1, Stmts,
                            {StmtRef, #drvstmt{ result   = {columns, Clms}
                                              , buf      = erlimem_buf:create(Fun)
                                              , maxrows  = MaxRows}
                            }),
                gen_server:reply(Form, Rslt),
                {noreply, State#state{stmts=NStmts}};
            Term ->
                ?Debug("LOCAL async __RX__ ~p For ~p", [Term, Form]),
                gen_server:reply(Form, Term),
                {noreply, State}
    end;
handle_info({StmtRef,{Rows,Completed}}, #state{stmts=Stmts}=State) when is_pid(StmtRef) ->
    {_, #drvstmt{buf=Buffer} = Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    case {Rows,Completed} of
        {[], _} ->
            ?Info([session, self()], "~p async_resp []", [?MODULE]),
            {noreply, State};
        {error, Result} ->
            ?Error([session, self()], "~p async_resp ~p", [?MODULE, Result]),
            {noreply, State};
        {Rows, Completed} when Completed =:= true; Completed =:= false ->
            erlimem_buf:insert_rows(Buffer, Rows),
            io:format(user, "~p Completed ~p~n", [StmtRef, Completed]),
            NewStmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{completed=Completed}}),
            {noreply, State#state{stmts=NewStmts}};
        Unknown ->
            ?Error([session, self()], "~p async_resp unknown resp ~p", [?MODULE, Unknown]),
            {noreply, State}
    end;

% 
% tcp
%
handle_info({tcp,S,Pkt}, #state{event_pids=EvtPids, buf=Buf, pending=Form, stmts=Stmts,maxrows=MaxRows}=State) ->
    NewBin = << Buf/binary, Pkt/binary >>,
    NewState =
    case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Debug("~p RX ~p byte of term, waiting...", [?MODULE, byte_size(Pkt)]),
                State#state{buf=NewBin};
            {error, Exception} ->
                ?Error("~p throw ~p", [?MODULE, Exception]),
                NewBuf = <<>>,
                throw(Exception);
            {ok, Clms, Fun, StmtRef} ->
                ?Debug("~p RX ~p", [?MODULE, {Clms, Fun, StmtRef}]),
                Rslt = {ok, Clms, {?MODULE, StmtRef, self()}},
                NStmts = lists:keystore(StmtRef, 1, Stmts,
                            {StmtRef, #drvstmt{ result   = {columns, Clms}
                                              , buf      = erlimem_buf:create(Fun)
                                              , maxrows  = MaxRows}
                            }),
                gen_server:reply(Form, Rslt),
                State#state{buf= <<>>, stmts=NStmts};
            {StmtRef,{Rows,Completed}} when is_pid(StmtRef) ->
                ?Info("TCP async __RX__ rows ~p For ~p", [length(Rows), Form]),
                {_, NState} = handle_info({StmtRef,{Rows,Completed}}, State),
                NState#state{buf= <<>>};
            Term ->
                ?Info("TCP async __RX__ ~p For ~p", [Term, Form]),
                gen_server:reply(Form, Term),
                State#state{buf= <<>>}
    end,
    inet:setopts(S,[{active,once}]),
    {noreply, NewState};

%
% MNESIA Events
%
handle_info({_,{complete, _}} = Evt, #state{event_pids=EvtPids}=State) ->
    case lists:keyfind(activity, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "~p # ~p <- ~p", [?MODULE, Found, Evt])
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
                    ?Debug([session, self()], "~p # ~p <- ~p", [?MODULE, Found, Evt])
            end
    end,
    {noreply, State};
handle_info({_,{D,Tab,_,_,_}} = Evt, #state{event_pids=EvtPids}=State) when D =:= write;
                                                                            D =:= delete ->
    case lists:keyfind({table, Tab, detailed}, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "~p # ~p <- ~p", [?MODULE, Found, Evt])
    end,
    {noreply, State};
handle_info(timeout, State) ->
    ?Debug([session, self()], "~p close on timeout", [?MODULE]),
    close({?MODULE, self()}),
    {noreply, State};
handle_info(Info, State) ->
    ?Error([session, self()], "~p unknown info ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{conn_param={Type, Opts},idle_timer=Timer}) ->
    erlang:cancel_timer(Timer),
    ?Debug([session, self()], "~p stopped ~p from ~p", [?MODULE, self(), {Type, Opts}]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.
