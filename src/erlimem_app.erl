-module(erlimem_app).
-behaviour(application).
-behaviour(supervisor).

%% Application callbacks
-export([start/2, stop/1]).

%% Supervisor callback
-export([init/1]).

start(_Type, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) -> ok.

%% @doc Supervisor Callback
%% @hidden
init(_) ->
    {ok, {{one_for_one,3, 10}, []}}.
