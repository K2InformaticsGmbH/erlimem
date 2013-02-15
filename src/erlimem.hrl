-define(IMEM_TIMEOUT, 1000).
-define(SESSION_TIMEOUT, 3600000).
-define(MAX_PREFETCH_ON_FLIGHT, 5).

-define(H,  element(1,erlang:time())).
-define(M,  element(2,erlang:time())).
-define(S,  element(3,erlang:time())).
-define(MS, element(3, erlang:now()) div 1000 rem 1000).

-define(T, lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B.~3..0B", [?H,?M,?S,?MS]))).

-ifndef(NOLOGGING).
-define(LOG(__L,__M,__F,__A),
    case application:get_env(erlimem, logging) of
    {ok, debug} ->
        case __L of
        dbg -> io:format(user, ?T++" [debug ~p:~p] [_DRVR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        nfo -> io:format(user, ?T++" [info  ~p:~p] [_DRVR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        err -> io:format(user, ?T++" [error ~p:~p] [_DRVR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        _ -> ok
        end;
    {ok, info} ->
        case __L of
        nfo -> io:format(user, ?T++" [info] [_DRVR_] " ++ __F ++ "~n", __A);
        err -> io:format(user, ?T++" [error] [_DRVR_] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    {ok, error} ->
        case __L of
        err -> io:format(user, ?T++" [error] [_DRVR_] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    _ -> ok
    end
).
-else.
-define(LOG(__L,__M,__F,__A), ok = ok).
-endif.

-define(Debug(__M,__F,__A), ?LOG(dbg, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F,__A),     ?LOG(dbg,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F),         ?LOG(dbg,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(Info(__M,__F,__A),  ?LOG(nfo, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F,__A),      ?LOG(nfo,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F),          ?LOG(nfo,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(Error(__M,__F,__A), ?LOG(err, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F,__A),     ?LOG(err,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F),         ?LOG(err,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(NoRefSqlRegEx, "^(?i)(CREATE|INSERT|UPDATE|DELETE|DROP)").
