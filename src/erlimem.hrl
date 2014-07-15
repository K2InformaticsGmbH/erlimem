-define(IMEM_TIMEOUT, infinity).
-define(SESSION_TIMEOUT, 3600000).
-define(MAX_PREFETCH_ON_FLIGHT, 5).

-define(LOG_TAG, "_IMDR_").

-define(Debug(__M,__F,__A), lager:debug(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F,__A),     lager:debug(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F),         lager:debug(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Info(__M,__F,__A),  lager:info(__M,  "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F,__A),      lager:info(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F),          lager:info(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Error(__M,__F,__A), lager:error(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F,__A),     lager:error(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F),         lager:error(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(NoRefSqlRegEx, "^(?i)(CREATE|INSERT|UPDATE|DELETE|DROP)").
