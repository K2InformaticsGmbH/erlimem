-module(erlimem).

%% Application callbacks
-export([start/0, stop/0]).

start() ->
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend, info},
                                          {lager_file_backend,
                                           [{"error.log", error, 10485760, "$D0", 5},
                                            {"console.log", info, 10485760, "$D0", 5}]}]),
    application:set_env(lager, error_logger_redirect, false),
    lager:start(),
    application:start(?MODULE).

stop()  ->
    application:stop(?MODULE).
