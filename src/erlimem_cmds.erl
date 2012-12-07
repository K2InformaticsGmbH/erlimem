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
    Args = case Fun of
        fetch_recs_async -> Args0 ++ [self()];
        _                -> Args0
    end,
    try
        case Media of
            undefined ->
                Res = case Node of
                    Node when Node =:= node() -> apply(Mod, Fun, Args);
                    _ -> rpc:call(Node, Mod, Fun, Args)
                end,
                case Fun of
                    fetch_recs_async -> recv_msg();
                    _ -> Res
                end;
            Socket ->
                gen_tcp:send(Socket, term_to_binary([Mod,Fun|Args])),
                recv_tcp(Socket, <<>>)
        end
    catch
        _Class:Result -> {error, Result, erlang:get_stacktrace()}
    end.

recv_msg() ->
    receive
        Data -> Data
    end.

recv_tcp(Sock, Bin) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Pkt} ->
        NewBin = << Bin/binary, Pkt/binary >>,
        case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                io:format(user, "term incomplete, received ~p bytes waiting...~n", [byte_size(Pkt)]),
                recv_tcp(Sock, NewBin);
            Data ->
                %io:format(user, "Got ~p~n", [Data]),
                Data
        end;
    {error, Reason} ->
        throw({error, {"TCP receive error", Reason}})
    end.
