-module(erlimem_cmds).

-export([exec/2]).

exec(Cmd, {tcp, Socket}) ->
    gen_tcp:send(Socket, term_to_binary(Cmd)),
    recv_term(Socket, <<>>);
exec(CmdTuple, {rpc, Node}) ->
    exec_catch(Node, imem_sec, CmdTuple);
exec(CmdTuple, {local_sec, _}) ->
    exec_catch(node(), imem_sec, CmdTuple);
exec(CmdTuple, {local, _}) ->
    exec_catch(node(), imem_meta, CmdTuple).

exec_catch(Node, Mod, CmdTuple) when Node =:= node() ->
    {Cmd, Args} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),
    try
        apply(Mod, Fun, Args)
    catch
        _Class:Result -> {error, Result}
    end;
exec_catch(Node, Mod, CmdTuple) ->
    {Cmd, Args} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),
    try
        rpc:call(Node, Mod, Fun, Args)
    catch
        _Class:Result -> {error, Result}
    end.

recv_term(Sock, Bin) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Pkt} ->
        NewBin = << Bin/binary, Pkt/binary >>,
        case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                io:format(user, "term incomplete, received ~p bytes waiting...~n", [byte_size(Pkt)]),
                recv_term(Sock, NewBin);
            Data -> Data
        end;
    {error, Reason} ->
            io:format(user, "TCP receive error ~p!~n", [Reason])
    end.
