-module(erlimem_cmds).

-include("erlimem.hrl").

-export([exec/3, recv_sync/3]).

exec(Ref, CmdTuple, {tcp, Socket}) ->
    exec_catch(Ref, Socket, undefined, imem_sec, CmdTuple);
exec(Ref, CmdTuple, {rpc, Node}) ->
    exec_catch(Ref, undefined, Node, imem_sec, CmdTuple);
exec(Ref, CmdTuple, {local_sec, _}) ->
    exec_catch(Ref, undefined, node(), imem_sec, CmdTuple);
exec(Ref, CmdTuple, {local, _}) ->
    {[Cmd|_], Args} = lists:split(1, tuple_to_list(CmdTuple)),
    exec_catch(Ref, undefined, node(), imem_meta, list_to_tuple([Cmd|lists:nthtail(1, Args)])).

exec_catch(Ref, Media, Node, Mod, CmdTuple) ->
    {Cmd, Args0} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),

    Args = case Fun of
        fetch_recs_async -> Args0 ++ [self()];
        _                -> Args0
    end,
    try
        case Media of
            undefined ->
                ?Debug("LOCAL ___TX___ ~p", [{Node, Mod, Fun, Args}]),
                case Node of
                    Node when Node =:= node() ->
                        ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Mod, Fun, Args}]),
                        ok = apply(imem_server, mfa, [{Ref, Mod, Fun, Args}, {self(), Ref}]);
                    _ ->
                        ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Node, Mod, Fun, Args}]),
                        ok = rpc:call(Node, imem_server, mfa, [{Ref, Mod, Fun, Args}, {self(), Ref}])
                end;
            Socket ->
                ?Debug([session, self()], "TCP ___TX___ ~p", [{Mod, Fun, Args}]),
                ReqBin = term_to_binary({Ref,Mod,Fun,Args}),
                PayloadSize = byte_size(ReqBin),
                gen_tcp:send(Socket, << PayloadSize:32, ReqBin/binary >>)
        end
    catch
        _Class:Result ->
            throw({{error, Result}, erlang:get_stacktrace()})
    end.

recv_sync({M, _}, _, _) when M =:= rpc; M =:= local; M =:= local_sec ->
    receive
        {_, {error, Exception}} ->
            ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
            throw({{error, Exception}, erlang:get_stacktrace()});
        Data ->
            ?Debug("LOCAL ___RX___ ~p", [Data]),
            Data
    end;
recv_sync({tcp, Sock}, Bin, Len) ->
    case gen_tcp:recv(Sock, 0) of
    {ok, Pkt} ->
        {NewLen, NewBin} =
            if Bin =:= <<>> ->
                << L:32, PayLoad/binary >> = Pkt,
                LenBytes = << L:32 >>,
                ?Info(" term size ~p~n", [LenBytes]),
                {L, PayLoad};
            true -> {Len, <<Bin/binary, Pkt/binary>>}
        end,
        case {byte_size(NewBin), NewLen} of
        {NewLen, NewLen} ->
            case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Info("~p RX ~p byte of term, waiting...", [?MODULE, byte_size(Pkt)]),
                recv_sync({tcp, Sock}, NewBin, NewLen);
            {_, {error, Exception}} ->
                ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
                throw({{error, Exception}, erlang:get_stacktrace()});
            {error, Exception} ->
                ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
                throw({{error, Exception}, erlang:get_stacktrace()});
            Term ->
                ?Debug("TCP ___RX___ ~p", [Term]),
                Term
            end;
        _ ->
            ?Info(" [INCOMPLETE] ~p received ~p of ~p bytes buffering...", [self(), byte_size(NewBin), NewLen]),
            recv_sync({tcp, Sock}, NewBin, NewLen)
        end;
    {error, Reason} ->
        ?Error("~p tcp error ~p", [?MODULE, Reason]),
        ?Error("~p tcp error stack :~n~p~n", [?MODULE, erlang:get_stacktrace()]),
        throw({{error, Reason}, erlang:get_stacktrace()})
    end.
