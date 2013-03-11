-define(IMEM_TIMEOUT, 1000).
-define(SESSION_TIMEOUT, 3600000).
-define(MAX_PREFETCH_ON_FLIGHT, 5).

-include("log.hrl").
-define(LOG_TAG, "_IMDR_").

-define(Debug(__M,__F,__A), ?LOG(?LOG_TAG, dbg, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F,__A),     ?LOG(?LOG_TAG, dbg,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F),         ?LOG(?LOG_TAG, dbg,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(Info(__M,__F,__A),  ?LOG(?LOG_TAG, nfo, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F,__A),      ?LOG(?LOG_TAG, nfo,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F),          ?LOG(?LOG_TAG, nfo,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(Error(__M,__F,__A), ?LOG(?LOG_TAG, err, __M, "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F,__A),     ?LOG(?LOG_TAG, err,  [], "~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F),         ?LOG(?LOG_TAG, err,  [], "~p "++__F, [{?MODULE,?LINE}])).

-define(NoRefSqlRegEx, "^(?i)(CREATE|INSERT|UPDATE|DELETE|DROP)").
