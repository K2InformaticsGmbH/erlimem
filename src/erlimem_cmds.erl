-module(erlimem_cmds).

-export([exec/2]).

exec(Cmd, {tcp, Socket}) ->
    gen_tcp:send(Socket, term_to_binary(Cmd)),
    recv_term(Socket, <<>>);
exec(CmdTuple, {rpc, Node}) ->
    [Cmd, Args] = lists:split(1, tuple_to_list(CmdTuple)),
    rpc:call(Node, imem_if, Cmd, Args);
exec(CmdTuple, {local, _}) ->
    [Cmd, Args] = lists:split(1, tuple_to_list(CmdTuple)),
    apply(imem_if, Cmd, Args).

recv_term(Sock, Bin) ->
    {ok, Pkt} = gen_tcp:recv(Sock, 0),
    NewBin = << Bin/binary, Pkt/binary >>,
    case (catch binary_to_term(NewBin)) of
        {'EXIT', Reason} ->
            io:format(user, "term incomplete (~p), received ~p bytes~n", [Reason, byte_size(Pkt)]),
            recv_term(Sock, NewBin);
        Data -> Data
    end.

