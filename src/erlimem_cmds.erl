-module(erlimem_cmds).

-export([exec/2]).

exec(Cmd, {tcp, Socket}) ->
    gen_tcp:send(Socket, term_to_binary(Cmd)),
    recv_term(Socket, <<>>);
exec(CmdTuple, {rpc, Node}) ->
    {Cmd, Args} = lists:split(1, tuple_to_list(CmdTuple)),
    rpc:call(Node, imem_sec, lists:nth(1, Cmd), Args);
exec(CmdTuple, {local_sec, _}) ->
    {Cmd, Args} = lists:split(1, tuple_to_list(CmdTuple)),
    apply(imem_sec, lists:nth(1, Cmd), Args);
exec(CmdTuple, {local, _}) ->
    {Cmd, Args} = lists:split(1, tuple_to_list(CmdTuple)),
    apply(imem_meta, lists:nth(1, Cmd), Args).

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
