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
        dbg -> io:format(user, ?T++" [debug ~p:~p] [_IMDR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        nfo -> io:format(user, ?T++" [info  ~p:~p] [_IMDR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        err -> io:format(user, ?T++" [error ~p:~p] [_IMDR_] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        _ -> ok
        end;
    {ok, info} ->
        case __L of
        nfo -> io:format(user, ?T++" [info] [_IMDR_] " ++ __F ++ "~n", __A);
        err -> io:format(user, ?T++" [error] [_IMDR_] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    {ok, error} ->
        case __L of
        err -> io:format(user, ?T++" [error] [_IMDR_] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    _ -> ok
    end
).
-else.
-define(LOG(__L,__M,__F,__A), ok = ok).
-endif.
