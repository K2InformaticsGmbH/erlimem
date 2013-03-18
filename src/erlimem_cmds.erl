-module(erlimem_cmds).

-include("erlimem.hrl").

-export([exec/2, recv_sync/2]).

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
                ?Debug("LOCAL ___TX___ ~p", [{Node, Mod, Fun, Args}]),
                case Node of
                    Node when Node =:= node() ->
                        ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Mod, Fun, Args}]),
                        ExecRes = apply(Mod, Fun, Args),
                        ?Debug([session, self()], "~p MFA ~p -> ~p", [?MODULE, {Mod, Fun, Args}, ExecRes]),
                        self() ! {resp, ExecRes};
                    _ ->
                        ?Debug([session, self()], "~p MFA ~p", [?MODULE, {Node, Mod, Fun, Args}]),
                        self() ! rpc:call(Node, Mod, Fun, Args)
                end;
            Socket ->
                ?Debug([session, self()], "TCP ___TX___ ~p", [{Mod, Fun, Args}]),
                gen_tcp:send(Socket, term_to_binary([Mod,Fun|Args]))
        end
    catch
        _Class:Result ->
            throw({{error, Result}, erlang:get_stacktrace()})
    end.

recv_sync({M, _}, _) when M =:= rpc; M =:= local; M =:= local_sec ->
    receive
        Data ->
            ?Debug("LOCAL ___RX___ ~p", [Data]),
            Data
    end;
recv_sync({tcp, Sock}, Bin) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Pkt} ->
        NewBin = << Bin/binary, Pkt/binary >>,
        case (catch binary_to_term(NewBin)) of
            {'EXIT', _Reason} ->
                ?Debug("~p RX ~p byte of term, waiting...", [?MODULE, byte_size(Pkt)]),
                recv_sync({tcp, Sock}, NewBin);
            {error, Exception} ->
                ?Error("~p throw ~p", [?MODULE, Exception]),
                throw({{error, Exception}, erlang:get_stacktrace()});
            Term ->
                ?Debug("TCP ___RX___ ~p", [Term]),
                {resp, Term}
        end;
    {error, Reason} ->
        ?Error("~p tcp error ~p", [?MODULE, Reason]),
        ?Debug("~p tcp error stack ~p", [?MODULE, erlang:get_stacktrace()]),
        throw({{error, Reason}, erlang:get_stacktrace()})
    end.
