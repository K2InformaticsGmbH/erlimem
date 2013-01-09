-define(IMEM_TIMEOUT, 1000).
-define(SESSION_TIMEOUT, 3600000).

-define(T,
    fun() ->
        {_, _, Micro} = erlang:now(),
        Ms = Micro / 1000000,
        {H,M,S} = erlang:time(),
        lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B.~p", [H,M,S,Ms]))
    end
).

-ifndef(NOLOGGING).
-define(LOG(__L,__M,__F,__A),
    case application:get_env(erlimem, logging) of
    {ok, debug} ->
        case __L of
        dbg -> io:format(user, "[debug] " ++ __F ++ "~n", __A);
        nfo -> io:format(user, "[info] " ++ __F ++ "~n", __A);
        err -> io:format(user, "[error] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    {ok, info} ->
        case __L of
        nfo -> io:format(user, "[info] " ++ __F ++ "~n", __A);
        err -> io:format(user, "[error] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    {ok, error} ->
        case __L of
        err -> io:format(user, "[error] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    _ -> ok
    end
).
-else.
-define(LOG(__L,__M,__F,__A), ok = ok).
-endif.

-define(Debug(__M,__F,__A), ?LOG(dbg, __M,__F,__A)).
-define(Debug(__F,__A),     ?LOG(dbg, [], __F,__A)).
-define(Debug(__F),         ?LOG(dbg, [], __F, [])).

-define(Info(__M,__F,__A),  ?LOG(nfo, __M,__F,__A)).
-define(Info(__F,__A),      ?LOG(nfo, [], __F,__A)).
-define(Info(__F),          ?LOG(nfo, [], __F, [])).

-define(Error(__M,__F,__A), ?LOG(err, __M,__F,__A)).
-define(Error(__F,__A),     ?LOG(err, [], __F,__A)).
-define(Error(__F),         ?LOG(err, [], __F, [])).
