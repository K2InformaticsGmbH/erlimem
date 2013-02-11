-module(erlimem_session).
-behaviour(gen_server).

-include("erlimem.hrl").
-include_lib("imem/include/imem_sql.hrl").

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
        completed,
        fetchopts,
        fetchonfly = 1
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
-export([ start_async_read/2
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
        , prepare_update/1
        , execute_update/1
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
prepare_update(              {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {prepare_update, StmtRef}).
execute_update(              {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {execute_update, StmtRef}).
row_with_key(RowId,          {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {row_with_key, StmtRef, RowId}).
fetch_close(                 {?MODULE, StmtRef, Pid}) -> gen_server:call(Pid, {fetch_close, StmtRef}).
start_async_read(Opts,       {?MODULE, StmtRef, Pid}) ->
    ok = gen_server:call(Pid, {clear_buf, StmtRef}),
    gen_server:cast(Pid, {read_block_async, Opts, StmtRef}).

call(Pid, Msg) ->
    ?Debug("~p call ~p ~p", [{?MODULE,?LINE}, Pid, Msg]),
    gen_server:call(Pid, Msg, ?IMEM_TIMEOUT).

init([Type, Opts, {User, Pswd}]) when is_binary(User), is_binary(Pswd) ->
    ?Debug("~p connecting with ~p cred ~p", [{?MODULE,?LINE}, {Type, Opts}, {User, Pswd}]),
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
                        ?Info("authenticated ~p -> ~p", [{User, Pswd}, S]),
                        erlimem_cmds:exec({login,S}, Connect),
                        {resp, S} = erlimem_cmds:recv_sync(Connect, <<>>),
                        ?Info("logged in ~p", [{User, S}]),
                        case Connect of
                            {tcp,Sck} -> inet:setopts(Sck,[{active,once}]);
                            _ -> ok
                        end,
                        S
                end,
                ?Info("~p started ~p connected to ~p", [{?MODULE,?LINE}, self(), {Type, Opts}]),
                Timer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
                {ok, #state{connection=Connect, schema=Schema, conn_param={Type, Opts}, idle_timer=Timer, seco=SeCo}}
            catch
            _Class:{Result,ST} ->
                ?Error("~p erlimem connect error ~p", [{?MODULE,?LINE}, Result]),
                ?Debug("~p erlimem connect error stackstrace ~p", [{?MODULE,?LINE}, ST]),
                case Connect of
                    {tcp, Sock} -> gen_tcp:close(Sock);
                    _ -> ok
                end,
                {stop, Result}
            end;
        {error, Reason} ->
            ?Error("~p erlimem connect error ~p", [{?MODULE,?LINE}, Reason]),
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
            ?Debug("~p delete ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
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
    #drvstmt{buf=Buf,completed=Completed} = Stmt,
    NewBuf = if Completed =:= true -> erlimem_buf:clear(Buf); true -> Buf end,
    NewStmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    ?Debug("~p replace ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,ok,State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({get_buffer_max, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf,completed=C} = Stmt,
    {ok, Count} = erlimem_buf:get_buffer_max(Buf),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    Completed = if (C =:= true) orelse (C =:= false) -> C; true -> false end,
    {reply, {ok,Completed,Count}, State#state{idle_timer=NewTimer}};
handle_call({rows_from, StmtRef, RowId}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_,
        #drvstmt{ buf = Buf
                , maxrows = MaxRows
                , fetchonfly = FReq
                , fetchopts = Opts
                , completed = Status} = Stmt
    } = lists:keyfind(StmtRef, 1, Stmts),
    {{Rows,_,CatchSize}, NewBuf} = erlimem_buf:get_rows_from(Buf, RowId, MaxRows),
    NewFReq = if 
        (FReq < ?MAX_PREFETCH_ON_FLIGHT)  -> FReq + 1;
        (FReq == ?MAX_PREFETCH_ON_FLIGHT) -> ?MAX_PREFETCH_ON_FLIGHT;
        true                              -> FReq
    end,
    ?Debug("fetch on flight ~p", [NewFReq]),
    if
        (FReq =:= 0) andalso (NewFReq > 0) ->
            ?Info("~p ~p prefetching...", [{?MODULE,?LINE}, StmtRef]),
            gen_server:cast(self(), {read_block_async, Opts, StmtRef});
        true -> ok
    end,
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    ?Debug("~p add ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({prev_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, Stmt} = lists:keyfind(StmtRef, 1, Stmts),
    #drvstmt{buf=Buf, maxrows=MaxRows, completed = Status} = Stmt,
    {{Rows,_,CatchSize}, NewBuf} = erlimem_buf:get_prev_rows(Buf, MaxRows),
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf}}),
    ?Debug("~p add ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
handle_call({next_rows, StmtRef}, _From, #state{idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_,
        #drvstmt{ buf = Buf
                , maxrows = MaxRows
                , fetchonfly = FReq
                , fetchopts = Opts
                , completed = Status} = Stmt
    } = lists:keyfind(StmtRef, 1, Stmts),
    {{Rows,_,CatchSize}, NewBuf} = erlimem_buf:get_next_rows(Buf, MaxRows),
    NewFReq = if 
        (FReq < ?MAX_PREFETCH_ON_FLIGHT)  -> FReq + 1;
        (FReq == ?MAX_PREFETCH_ON_FLIGHT) -> ?MAX_PREFETCH_ON_FLIGHT;
        true                              -> FReq
    end,
    ?Debug("fetch on flight ~p", [NewFReq]),
    if
        (FReq =:= 0) andalso (NewFReq > 0) ->
            ?Info("~p ~p prefetching...", [{?MODULE,?LINE}, StmtRef]),
            gen_server:cast(self(), {read_block_async, Opts, StmtRef});
        true -> ok
    end,
    NewStmts = lists:keystore(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{buf=NewBuf, fetchonfly = NewFReq}}),
    ?Debug("~p add/replace ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,{Rows,Status,CatchSize},State#state{idle_timer=NewTimer,stmts=NewStmts}};
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
    ?Info("~p fetch_close ~p", [{?MODULE,?LINE}, StmtRef]),
    erlang:cancel_timer(Timer),
    erlimem_cmds:exec({fetch_close, SeCo, StmtRef}, Connection),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {reply,ok,State#state{idle_timer=NewTimer}};
handle_call({prepare_update, StmtRef}, From, #state{connection=Connection,seco=SeCo,idle_timer=Timer,stmts=Stmts} = State) ->
    erlang:cancel_timer(Timer),
    {_, #drvstmt{buf=Buf}} = lists:keyfind(StmtRef, 1, Stmts),
    Rows = erlimem_buf:get_modified_rows(Buf),
    ?Debug([session, self()], "~p modified rows ~p", [{?MODULE,?LINE}, Rows]),
    erlimem_cmds:exec({update_cursor_prepare, SeCo, StmtRef, Rows}, Connection),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {noreply, State#state{idle_timer=NewTimer,pending=From}};
handle_call({execute_update, StmtRef}, From, #state{connection=Connection,seco=SeCo,idle_timer=Timer} = State) ->
    erlang:cancel_timer(Timer),
    erlimem_cmds:exec({update_cursor_execute, SeCo, StmtRef, optimistic}, Connection),
    NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
    {noreply, State#state{idle_timer=NewTimer,pending=From}};

handle_call(Msg, {From, _} = Frm, #state{connection=Connection
                                        ,idle_timer=Timer
                                        ,schema=Schema
                                        ,seco=SeCo
                                        ,event_pids=EvtPids} = State) ->
    ?Debug("~p call ~p", [{?MODULE,?LINE}, Msg]),
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
        _Result ->
            NewTimer = erlang:send_after(?SESSION_TIMEOUT, self(), timeout),
            {noreply,State#state{idle_timer=NewTimer, event_pids=NewEvtPids,pending=Frm,maxrows=MaxRows}}
    end.

handle_cast({read_block_async, Opts, StmtRef}, #state{stmts=Stmts, connection=Connection, seco=SeCo}=State) ->
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #drvstmt{completed = Completed, fetchopts=OldOpts} = Stmt} ->
            if (Completed =:= true) andalso (Opts =/= OldOpts) ->
%            if (Completed =:= true) ->
                ?Info("~p ~p fetch_close", [{?MODULE,?LINE}, StmtRef]),
                erlimem_cmds:exec({fetch_close, SeCo, StmtRef}, Connection);
                true -> ok
            end,
            FetchMode = proplists:get_value(fetch_mode, Opts, none),
            TailMode  = proplists:get_value(tail_mode,  Opts, none),
            ?Info("Completed ~p, FetchMode ~p, TailMode ~p", [Completed, FetchMode, TailMode]),
            Fetch = case {Completed, FetchMode, TailMode} of
                {undefined,  none, none}  -> true;
                {undefined,  push, true}  -> true;
                {undefined,  push, none}  -> true;
                {undefined,  skip, true}  -> true;
                {undefined,  skip, none}  -> true;

                {true,       none, none}  -> false;
                {true,       push, true}  -> true;
                {true,       push, false} -> true;
                {true,       push, none}  -> true;
                {true,       skip, true}  -> true;
                {true,       skip, none}  -> true;

                {false,      none, none}  -> true;
                {false,      push, true}  -> true;
                {false,      push, false} -> true;
                {false,      push, none}  -> true;
                {false,      skip, true}  -> true;
                {false,      skip, none}  -> true;

                {tail,       none, none}  -> true;
                {tail,       push, true}  -> false;
                {tail,       push, false} -> false;
                {tail,       push, none}  -> true;
                {tail,       skip, true}  -> true;
                {tail,       skip, none}  -> true
            end,
            if Fetch andalso (Opts =/= OldOpts) ->
                ?Info("~p ~p fetch_recs_async ~p", [{?MODULE,?LINE}, StmtRef, Opts]),
                erlimem_cmds:exec({fetch_recs_async, SeCo, Opts, StmtRef}, Connection);
            true -> ok
            end,
            {noreply, State#state{stmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{fetchopts=Opts}})}};
        false ->
            ?Error("~p statement ~p not found in ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- Stmts]]),
            {noreply, State}
    end;
handle_cast(Request, State) ->
    ?Error([session, self()], "~p unknown cast ~p", [{?MODULE,?LINE}, Request]),
    {noreply, State}.

% 
% local / rpc / tcp fallback
%
handle_info({resp,Resp}, #state{pending=Form, stmts=Stmts,maxrows=MaxRows}=State) ->
    case Resp of
            {error, Exception} ->
                ?Error("~p throw ~p", [{?MODULE,?LINE}, Exception]),
                throw(Exception);
            {ok, #stmtResult{stmtCols = Clms, rowFun = Fun, stmtRef = StmtRef} = SRslt} ->
                ?Debug("~p RX ~p", [{?MODULE,?LINE}, SRslt]),
                Rslt = {ok, Clms, {?MODULE, StmtRef, self()}},
                NStmts = lists:keystore(StmtRef, 1, Stmts,
                            {StmtRef, #drvstmt{ result   = {columns, Clms}
                                              , buf      = erlimem_buf:create(Fun)
                                              , maxrows  = MaxRows}
                            }),
                ?Debug("~p statement ~p stored in ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NStmts]]),
                gen_server:reply(Form, Rslt),
                {noreply, State#state{stmts=NStmts}};
            Term ->
                ?Debug("~p LOCAL async __RX__ ~p For ~p", [{?MODULE, ?LINE}, Term, Form]),
                gen_server:reply(Form, Term),
                {noreply, State}
    end;
handle_info({StmtRef,{Rows,Completed}}, #state{stmts=Stmts}=State) when is_pid(StmtRef) ->
    case lists:keyfind(StmtRef, 1, Stmts) of
        {_, #drvstmt{buf=Buffer, fetchopts = Opts, fetchonfly = FReq} = Stmt} ->
            case {Rows,Completed} of
                {error, Result} ->
                    ?Error([session, self()], "~p async_resp ~p", [{?MODULE,?LINE}, Result]),
                    {noreply, State};
                {Rows, Completed} when Completed =:= true;
                                       Completed =:= false;
                                       Completed =:= tail ->
                    erlimem_buf:insert_rows(Buffer, Rows),
                    {ok, Count} = erlimem_buf:get_buffer_max(Buffer),
                    ?Info("~p ~p __RX__ received rows ~p total in buffer ~p status ~p fetch on flight ~p",
                         [{?MODULE, ?LINE}, StmtRef, length(Rows), Count, Completed, FReq]),
                    if
                        (Completed =:= false) andalso (Opts =:= []) andalso (FReq > 0) ->
                            ?Info("~p ~p prefetching...", [{?MODULE,?LINE}, StmtRef]),
                            NewFReq = FReq - 1,
                            gen_server:cast(self(), {read_block_async, Opts, StmtRef});
                        (Completed =:= true) -> NewFReq = 0;
                        true -> NewFReq = 0
                    end,
                    NewStmts = lists:keyreplace(StmtRef, 1, Stmts, {StmtRef, Stmt#drvstmt{completed=Completed, fetchonfly = NewFReq}}),
                    ?Debug("~p replaced ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- NewStmts]]),
                    {noreply, State#state{stmts=NewStmts}};
                Unknown ->
                    ?Error([session, self()], "~p async_resp unknown resp ~p", [{?MODULE,?LINE}, Unknown]),
                    {noreply, State}
            end;
        false ->
            ?Error("~p statement ~p not found in ~p", [{?MODULE,?LINE}, StmtRef, [S|| {S,_} <- Stmts]]),
            {noreply, State}
    end;

% 
% tcp
%
handle_info({tcp,S,Pkt}, #state{buf=Buf, pending=Form, stmts=Stmts,maxrows=MaxRows}=State) ->
    NewBin = << Buf/binary, Pkt/binary >>,
    NewState =
    case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Debug("~p RX ~p byte of term, waiting...", [{?MODULE,?LINE}, byte_size(Pkt)]),
                State#state{buf=NewBin};
            {error, Exception} ->
                ?Error("~p throw ~p", [{?MODULE,?LINE}, Exception]),
                gen_server:reply(Form,  {error, Exception}),
                State;
            Term ->
                NState =
                case Term of
                    {ok, #stmtResult{stmtCols = Clms, rowFun = Fun, stmtRef = StmtRef} = SRslt} ->
                        ?Debug("~p RX ~p", [{?MODULE,?LINE}, SRslt]),
                        Rslt = {ok, Clms, {?MODULE, StmtRef, self()}},
                        NStmts = lists:keystore(StmtRef, 1, Stmts,
                                    {StmtRef, #drvstmt{ result   = {columns, Clms}
                                                      , buf      = erlimem_buf:create(Fun)
                                                      , maxrows  = MaxRows}
                                    }),
                        gen_server:reply(Form, Rslt),
                        ?Debug("~p add/replace ~p stmts ~p", [{?MODULE,?LINE}, StmtRef, [_S || {_S,_} <- NStmts]]),
                        State#state{stmts=NStmts};
                    {StmtRef,{Rows,Completed}} when is_pid(StmtRef) ->
                        {_, NS} = handle_info({StmtRef,{Rows,Completed}}, State),
                        NS;
                    _ ->
                        ?Debug("~p TCP async __RX__ ~p For ~p", [{?MODULE,?LINE}, Term, Form]),
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
% MNESIA Events
%
handle_info({_,{complete, _}} = Evt, #state{event_pids=EvtPids}=State) ->
    case lists:keyfind(activity, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "~p # ~p <- ~p", [{?MODULE,?LINE}, Found, Evt])
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
                    ?Debug([session, self()], "~p # ~p <- ~p", [{?MODULE,?LINE}, Found, Evt])
            end
    end,
    {noreply, State};
handle_info({_,{D,Tab,_,_,_}} = Evt, #state{event_pids=EvtPids}=State) when D =:= write;
                                                                            D =:= delete ->
    case lists:keyfind({table, Tab, detailed}, 1, EvtPids) of
        {_, Pid} when is_pid(Pid) -> Pid ! Evt;
        Found ->
            ?Debug([session, self()], "~p # ~p <- ~p", [{?MODULE,?LINE}, Found, Evt])
    end,
    {noreply, State};


handle_info(timeout, State) ->
    ?Debug([session, self()], "~p close on timeout", [{?MODULE,?LINE}]),
    close({?MODULE, self()}),
    {noreply, State};
handle_info(Info, State) ->
    ?Error([session, self()], "~p unknown info ~p", [{?MODULE,?LINE}, Info]),
    {noreply, State}.

terminate(_Reason, #state{conn_param={Type, Opts},idle_timer=Timer}) ->
    erlang:cancel_timer(Timer),
    ?Debug([session, self()], "~p stopped ~p from ~p", [{?MODULE,?LINE}, self(), {Type, Opts}]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.
