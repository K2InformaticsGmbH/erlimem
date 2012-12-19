-module(erlimem_cmds).

-export([exec/2]).

exec(CmdTuple, {tcp, Socket}) ->
    exec_catch(Socket, undefined, imem_sec, CmdTuple);
exec(CmdTuple, {rpc, Node}) ->
    exec_catch(undefined, Node, imem_sec, CmdTuple);
exec(CmdTuple, {local_sec, _}) ->
    exec_catch(undefined, node(), imem_sec, CmdTuple);
exec(CmdTuple, {local, _}) ->
    {[Cmd|_], Args} = lists:split(1, tuple_to_list(CmdTuple)),
    exec_catch(undefined, node(), imem_meta, list_to_tuple([Cmd|lists:nthtail(1, Args)])).

exec_catch(Media, Node, Mod, CmdTuple) ->
    {Cmd, Args0} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),

    RespPid = case Fun of
        fetch_recs_async when Media == undefined -> recv_async(self(), <<>>);
        fetch_recs_async -> recv_async(Media, <<>>);
        _ -> self()
    end,
    Args = case Fun of
        fetch_recs_async -> Args0 ++ [RespPid];
        _                -> Args0
    end,
    try
        case Media of
            undefined ->
                Res = case Node of
                    Node when Node =:= node() -> apply(Mod, Fun, Args);
                    _ -> rpc:call(Node, Mod, Fun, Args)
                end,
                if Fun =/= fetch_recs_async -> Res; true -> ok end;
            Socket ->
                gen_tcp:send(Socket, term_to_binary([Mod,Fun|Args])),
                if Fun =/= fetch_recs_async -> rcv_tcp_pkt(Socket, <<>>); true -> ok end
        end
    catch
        _Class:Result -> throw({Result, erlang:get_stacktrace()})
    end.

recv_async(Pid, _) when is_pid(Pid) ->
    spawn(fun() ->
        receive
            Data -> gen_server:cast(Pid, {async_resp,  Data})
        end
    end);
recv_async(Sock, Bin) ->
    Pid = self(),
    spawn(fun() ->
        Resp = rcv_tcp_pkt(Sock, Bin),
        gen_server:cast(Pid, {async_resp,  Resp})
    end).

rcv_tcp_pkt(Sock, Bin) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Pkt} ->
        NewBin = << Bin/binary, Pkt/binary >>,
        case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                io:format(user, "term incomplete, received ~p bytes waiting for more...~n", [byte_size(Pkt)]),
                rcv_tcp_pkt(Sock, NewBin);
            {error, Exception} ->
                throw(Exception);
            Term ->
                Term
        end;
    {error, Reason} ->
        throw({{error, Reason}, erlang:get_stacktrace()})
    end.
