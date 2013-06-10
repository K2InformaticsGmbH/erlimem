-define(IMEM_TIMEOUT, 1000).
-define(SESSION_TIMEOUT, 3600000).
-define(MAX_PREFETCH_ON_FLIGHT, 5).

-include("log.hrl").
-define(LOG_TAG, "_IMDR_").

-define(Debug(__M,__F,__A), ?LOG(?LOG_TAG, dbg, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Debug(__F,__A),     ?LOG(?LOG_TAG, dbg,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Debug(__F),         ?LOG(?LOG_TAG, dbg,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).

-define(Info(__M,__F,__A),  ?LOG(?LOG_TAG, nfo, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Info(__F,__A),      ?LOG(?LOG_TAG, nfo,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Info(__F),          ?LOG(?LOG_TAG, nfo,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).

-define(Error(__M,__F,__A), ?LOG(?LOG_TAG, err, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Error(__F,__A),     ?LOG(?LOG_TAG, err,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Error(__F),         ?LOG(?LOG_TAG, err,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).

-define(NoRefSqlRegEx, "^(?i)(CREATE|INSERT|UPDATE|DELETE|DROP)").

-record(fsmctx, { % fsm interface
                  id
                , stmtColsLen
                , rowFun
                , sortFun
                , sortSpec
                , block_length
                , fetch_recs_async_fun
                , fetch_close_fun
                , filter_and_sort_fun
                , update_cursor_prepare_fun
                , update_cursor_execute_fun
                }).

