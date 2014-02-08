-module(erlimem_app).
-behaviour(application).
-behaviour(supervisor).

-include("erlimem.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% Supervisor callback
-export([init/1]).

start(_Type, _Args) ->
    ssl:start(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ?Info("erlimem application stopped").

%% @doc Supervisor Callback
%% @hidden
init(_) ->
    ?Info("erlimem application started"),
    {ok, {{one_for_one,3, 10}, []}}.
