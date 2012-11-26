-module(erlimem_cmds).

-export([exec/2]).

exec(Cmd, {tcp, Socket}) ->
    gen_tcp:send(Socket, term_to_binary(Cmd)),
    recv_tcp(Socket, <<>>);
exec(CmdTuple, {rpc, Node}) ->
    exec_catch(Node, imem_sec, CmdTuple, true);
exec(CmdTuple, {local_sec, _}) ->
    exec_catch(node(), imem_sec, CmdTuple, true);
exec(CmdTuple, {local, _}) ->
    exec_catch(node(), imem_meta, CmdTuple, false).

exec_catch(Node, Mod0, CmdTuple, IsSec) ->
    {Cmd, Args0} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),
    {Mod, Args} = case {Fun, IsSec} of
        {fetch_recs, false} -> {Mod0, lists:nthtail(1, Args0) ++ [self()]};
        {fetch_recs, _}     -> {Mod0, Args0 ++ [self(), IsSec]};
        {_, false}          -> {Mod0, lists:nthtail(1, Args0)};
        {_, _}              -> {Mod0, Args0}
    end,
    io:format(user, "Mod ~p, Fun ~p, Args ~p~n", [Mod, Fun, Args]),
    try
        Res = case Node of
            Node when Node =:= node() -> apply(Mod, Fun, Args);
            _ -> rpc:call(Node, Mod, Fun, Args)
        end,
        case Fun of
            fetch_recs -> recv_msg(<<>>);
            _ -> Res
        end
    catch
        _Class:Result -> {error, Result}
    end.

recv_msg(Bin) ->
    receive
        Pkt when is_binary(Pkt) ->
            NewBin = << Bin/binary, Pkt/binary >>,
            case (catch binary_to_term(NewBin)) of
                {'EXIT', _Reason} ->
                    io:format(user, "term incomplete, received ~p bytes waiting...~n", [byte_size(Pkt)]),
                    recv_msg(NewBin);
                Data -> Data
            end
    end.

recv_tcp(Sock, Bin) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Pkt} ->
        NewBin = << Bin/binary, Pkt/binary >>,
        case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                io:format(user, "term incomplete, received ~p bytes waiting...~n", [byte_size(Pkt)]),
                recv_tcp(Sock, NewBin);
            Data -> Data
        end;
    {error, Reason} ->
            io:format(user, "TCP receive error ~p!~n", [Reason])
    end.
