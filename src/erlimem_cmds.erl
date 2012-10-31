-module(erlimem_cmds).

-export([exec/2]).

exec(Cmd, Socket) ->
    gen_tcp:send(Socket, term_to_binary(Cmd)),
    recv_term(Socket, <<>>).

recv_term(Sock, Bin) ->
    {ok, Pkt} = gen_tcp:recv(Sock, 0),
    NewBin = << Bin/binary, Pkt/binary >>,
    case (catch binary_to_term(NewBin)) of
        {'EXIT', Reason} ->
            io:format(user, "term incomplete (~p), received ~p bytes~n", [Reason, byte_size(Pkt)]),
            recv_term(Sock, NewBin);
        Data -> Data
    end.

