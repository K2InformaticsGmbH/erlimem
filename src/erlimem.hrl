-define(IMEM_TIMEOUT,           infinity).
-define(UNAUTHIDLETIMEOUT,      600000).
-define(SESSION_TIMEOUT,        3600000).
-define(MAX_PREFETCH_ON_FLIGHT, 5).

-define(LOG_TAG, "_IMDR_").

-define(Debug(__M,__F,__A),     lager:debug(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F,__A),         lager:debug(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Debug(__F),             lager:debug(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Info(__M,__F,__A),      lager:info(__M,  "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F,__A),          lager:info(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Info(__F),              lager:info(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Notice(__M,__F,__A),    lager:notice(__M,  "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Notice(__F,__A),        lager:notice(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Notice(__F),            lager:notice(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Warn(__M,__F,__A),      lager:warning(__M,  "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Warn(__F,__A),          lager:warning(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Warn(__F),              lager:warning(      "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Error(__M,__F,__A),     lager:error(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F,__A),         lager:error(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Error(__F),             lager:error(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Critical(__M,__F,__A),  lager:critical(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Critical(__F,__A),      lager:critical(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Critical(__F),          lager:critical(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Alert(__M,__F,__A),     lager:alert(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Alert(__F,__A),         lager:alert(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Alert(__F),             lager:alert(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(Emergency(__M,__F,__A), lager:emergency(__M, "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Emergency(__F,__A),     lager:emergency(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}]++__A)).
-define(Emergency(__F),         lager:emergency(     "["++?LOG_TAG++"] ~p "++__F, [{?MODULE,?LINE}])).

-define(NoRefSqlRegEx, "^(?i)(CREATE|INSERT|UPDATE|DELETE|DROP)").
