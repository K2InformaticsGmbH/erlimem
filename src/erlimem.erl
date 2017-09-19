-module(erlimem).

-behaviour(application).
-behaviour(supervisor).

-include("erlimem.hrl").

%% Application callbacks
-export([start/0, stop/0, start/2, stop/1]).

%% Supervisor callback
-export([init/1]).

%% Public APIs
-export([open/2, loglevel/1]).

-spec loglevel(atom()) -> ok.
loglevel(L) -> application:set_env(erlimem, logging, L).

%% ===================================================================
%% Application callbacks
%% ===================================================================
start() ->
    ok = application:start(?MODULE).

stop() ->
    ok = application:stop(?MODULE).

start(_Type, _Args) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
    ?Info("---------------------------------------------------"),
    ?Info(" STARTING ERLIMEM"),
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, _} = Success ->
            ?Info(" ERLIMEM STARTED"),
            ?Info("---------------------------------------------------"),
            Success;
        Error ->
            ?Info(" ERLIMEM FAILED TO START ~p", [Error]),
            Error
    end.

stop(_State) ->
	?Info("SHUTDOWN ERLIMEM"),
    ?Info("---------------------------------------------------").

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% Helper macro for declaring children of supervisor
-define(CHILD(I), {I, {I, start_link, []}, temporary, 5000, worker, [I]}).

init(_) ->
	SupFlags = {simple_one_for_one, 5, 10},
    {ok, {SupFlags, [?CHILD(erlimem_session)]}}.

%% ===================================================================
%% Public APIs
%% ===================================================================
-spec open(local | local_sec
           | {tcp, inet:ip_address() | inet:hostname(), inet:port_number()}
           | {tcp, inet:ip_address() | inet:hostname(), inet:port_number(),
              Opts::list()}, atom()) ->
    {ok, {erlimem_session, pid()}} | {error, term()}.
open(Connect, Schema) ->
    case supervisor:start_child(?MODULE, [Connect, Schema]) of
        {error, {error, Error}} -> {error, Error};
        {error, Error}          -> {error, Error};
        {ok, Pid}               -> {ok, {erlimem_session, Pid}}
    end.
