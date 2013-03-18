-define(T,
(fun() ->
    {_,_,__McS} = __Now = erlang:now(),
    {{__YYYY,__MM,__DD},{__H,__M,__S}} = calendar:now_to_local_time(__Now),
    lists:flatten(io_lib:format("~2..0B.~2..0B.~4..0B ~2..0B:~2..0B:~2..0B.~6..0B", [__DD,__MM,__YYYY,__H,__M,__S,__McS rem 1000000]))
end)()).

-ifndef(NOLOGGING).
-define(LOG(__T,__L,__M,__F,__A),
    case application:get_env(erlimem, logging) of
    {ok, debug} ->
        case __L of
        dbg -> io:format(user, ?T++" [debug ~p:~4..0B] ["++__T++"] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        nfo -> io:format(user, ?T++" [info  ~p:~4..0B] ["++__T++"] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        err -> io:format(user, ?T++" [error ~p:~4..0B] ["++__T++"] " ++ __F ++ "~n", [?MODULE, ?LINE] ++ __A);
        _ -> ok
        end;
    {ok, info} ->
        case __L of
        nfo -> io:format(user, ?T++" [info] ["++__T++"] " ++ __F ++ "~n", __A);
        err -> io:format(user, ?T++" [error] ["++__T++"] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    {ok, error} ->
        case __L of
        err -> io:format(user, ?T++" [error] ["++__T++"] " ++ __F ++ "~n", __A);
        _ -> ok
        end;
    _ -> ok
    end
).
-else.
-define(LOG(__T,__L,__M,__F,__A), ok = ok).
-endif.
