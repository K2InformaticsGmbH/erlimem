-module(erlimem_cmds).

-include("erlimem.hrl").

-export([exec/3, recv_sync/3]).

-spec exec(undefined | pid(), tuple(), {atom(), term()}) -> ok | {error, atom()}.
exec(Ref, CmdTuple, {rpc, Node}) when Node == node() ->
    exec(Ref, CmdTuple, local_sec);
exec(Ref, CmdTuple, {rpc, Node}) ->
    exec_catch(Ref, {rpc, Node}, imem_sec, CmdTuple);
exec(Ref, CmdTuple, local_sec) ->
    exec_catch(Ref, local_sec, imem_sec, CmdTuple);
exec(Ref, CmdTuple, local) ->
    {[Cmd|_], Args} = lists:split(1, tuple_to_list(CmdTuple)),
    exec_catch(Ref, local, imem_meta, list_to_tuple([Cmd|lists:nthtail(1, Args)]));
exec(Ref, CmdTuple, {gen_tcp, Socket}) ->
    exec_catch(Ref, {gen_tcp, Socket}, imem_sec, CmdTuple);
exec(Ref, CmdTuple, {ssl, Socket}) ->
    exec_catch(Ref, {ssl, Socket}, imem_sec, CmdTuple).

-spec exec_catch(undefined | pid(),
                 {rpc, atom()}
                 | {gen_tcp, gen_tcp:socket()}
                 | {ssl, ssl:sslsocket()},
                 imem_sec | imem_meta,
                 tuple()) -> ok | {error, atom()}.
exec_catch(Ref, Media, Mod, CmdTuple) ->
    {Cmd, Args0} = lists:split(1, tuple_to_list(CmdTuple)),
    Fun = lists:nth(1, Cmd),

    Args = case Fun of
        fetch_recs_async -> Args0 ++ [self()];
        _                -> Args0
    end,
    try
        case Media of
            Media when Media == local; Media == local_sec ->
                ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Mod, Fun, Args}]),
                ok = apply(imem_server, mfa, [{Ref, Mod, Fun, Args}, {self(), Ref}]);
            {rpc, Node} ->
                ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Node, Mod, Fun, Args}]),
                ok = rpc:call(Node, imem_server, mfa, [{Ref, Mod, Fun, Args}, {self(), Ref}]);
            {Transport, Socket} ->
                ?Debug([session, self()], "TCP ___TX___ ~p", [{Mod, Fun, Args}]),
                ReqBin = term_to_binary({Ref,Mod,Fun,Args}),
                PayloadSize = byte_size(ReqBin),
                Transport:send(Socket, << PayloadSize:32, ReqBin/binary >>)
        end
    catch
        _Class:Result ->
            throw({{error, Result}, erlang:get_stacktrace()})
    end.

-spec recv_sync({atom(), undefined | gen_tcp:socket() | ssl:socket()}, binary(), integer()) -> term().
recv_sync({M, _}, _, _) when M =:= rpc; M =:= local; M =:= local_sec ->
    receive
        {_, {error, Exception}} ->
            ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
            throw({error, Exception});
        Data ->
            ?Debug("LOCAL ___RX___ ~p", [Data]),
            Data
    end;
recv_sync({Mod, Sock}, Bin, Len) when Mod =:= ssl; Mod =:= gen_tcp ->
    case Mod:recv(Sock, 0) of
    {ok, Pkt} ->
        {NewLen, NewBin} =
            if Bin =:= <<>> ->
                << L:32, PayLoad/binary >> = Pkt,
                LenBytes = << L:32 >>,
                ?Debug("term size ~p~n", [LenBytes]),
                {L, PayLoad};
            true -> {Len, <<Bin/binary, Pkt/binary>>}
        end,
        case {byte_size(NewBin), NewLen} of
        {NewLen, NewLen} ->
            case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Info("~p RX ~p byte of term, waiting...", [?MODULE, byte_size(Pkt)]),
                recv_sync({Mod, Sock}, NewBin, NewLen);
            {_, {error, Exception}} ->
                ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
                throw({error, Exception});
            %{error, Exception} ->
            %    ?Error("~p throw exception :~n~p~n", [?MODULE, Exception]),
            %    throw({error, Exception});
            Term ->
                ?Debug("TCP ___RX___ ~p", [Term]),
                Term
            end;
        _ ->
            ?Info(" [INCOMPLETE] ~p received ~p of ~p bytes buffering...", [self(), byte_size(NewBin), NewLen]),
            recv_sync({Mod, Sock}, NewBin, NewLen)
        end;
    {error, Reason} ->
        ?Error("~p tcp error ~p", [?MODULE, Reason]),
        ?Error("~p tcp error stack :~n~p~n", [?MODULE, erlang:get_stacktrace()]),
        throw({{error, Reason}, erlang:get_stacktrace()})
    end.
