-module(erlimem_test).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {
        timeout, 1000000,
        {
            setup, fun setup/0, fun teardown/1,
            {with, [
                fun connects/1,
                fun login/1,
                fun mfa/1,
                fun large_echo/1
            ]}
        }
    }.

setup() ->
    ok = application:load(sasl),
    ok = application:set_env(sasl, sasl_error_logger, false),
    ok = application:load(imem),
    ok = application:set_env(imem, erl_cluster_mgrs, []),
    ok = application:set_env(imem, tcp_server, true),
    ok = application:set_env(imem, tcp_ip, "127.0.0.1"),
    ok = application:set_env(imem, tcp_port, 9999),
    ok = application:set_env(imem, purge_server, false),
    ok = application:set_env(imem, proll_server, false),
    ok = application:set_env(imem, client_server, false),
    ok = application:set_env(imem, if_sys_conf_server, false),
    ok = application:set_env(imem, domain_server, false),
    ok = application:set_env(imem, snap_server, false),
    ok = application:set_env(imem, default_admin_pswd, <<"erlimem_test">>),
    ok = application:set_env(imem, monitor_server, false),
    ok = application:set_env(imem, cold_start_recover, false),
    ok = application:set_env(imem, mnesia_node_type, ram),
    ok = application:set_env(imem, meta_server, true),
    ok = application:set_env(imem, mnesia_schema_name, erlimem),
    {ok, _} = application:ensure_all_started(imem),
    {ok, _} = application:ensure_all_started(erlimem),
    ok.

teardown(_) ->
    ok = application:stop(erlimem),
    ok = application:stop(imem).

connects(_) ->
    {ok, Session} = erlimem:open(local, erlimem),
    ?assertEqual(ok, Session:close()),
    ?assertMatch({erlimem_session, _}, Session),
    {ok, SessionSec} = erlimem:open(local_sec, erlimem),
    ?assertEqual(ok, SessionSec:close()),
    ?assertMatch({erlimem_session, _}, SessionSec),
    {ok, SessionTcp} = erlimem:open({tcp, "127.0.0.1", 9999}, erlimem),
    ?assertMatch({erlimem_session, _}, SessionTcp),
    ?assertEqual(ok, SessionTcp:close()).

login(_) ->
    SessionId = make_ref(),
    GoodCred = {pwdmd5, {<<"system">>, erlang:md5(<<"erlimem_test">>)}},
    BadCred = {pwdmd5, {<<"system">>, erlang:md5(<<"bad_password">>)}},

    % Plain TCP to IMEM SSL
    {ok, Session} = erlimem:open({tcp, "127.0.0.1", 9999}, erlimem),
    ?assertMatch({erlimem_session, _}, Session),
    ?assertException(
        exit,
        {normal, {gen_server,call,[_, {auth, erlimem, SessionId, GoodCred}, _]}},
        Session:auth(erlimem, SessionId, GoodCred)
    ),
    ?assertEqual(ok, Session:close()),

    % SSL to IMEM SSL
    {ok, SessionSsl} = erlimem:open({tcp, "127.0.0.1", 9999, [ssl]}, erlimem),
    ?assertException(
        throw, {{'SecurityException',_}, _},
        SessionSsl:auth(erlimem, SessionId, BadCred)
    ),
    ?assertEqual(ok, SessionSsl:auth(erlimem, SessionId, GoodCred)),
    ?assertEqual(true, is_integer(SessionSsl:run_cmd(login,[]))),
    ?assertEqual(ok, SessionSsl:close()).

mfa(_) ->
    SessionId = make_ref(),
    GoodCred = {pwdmd5, {<<"system">>, erlang:md5(<<"erlimem_test">>)}},

    {ok, SessionSsl} = erlimem:open({tcp, "127.0.0.1", 9999, [ssl]}, erlimem),
    ok = SessionSsl:auth(erlimem, SessionId, GoodCred),
    ?assertEqual(true, is_integer(SessionSsl:run_cmd(login,[]))),
    ?assertEqual(true, is_list(SessionSsl:run_cmd(all_tables,[]))),
    ?assertEqual(ok, SessionSsl:close()).

large_echo(_) ->
    SessionId = make_ref(),
    GoodCred = {pwdmd5, {<<"system">>, erlang:md5(<<"erlimem_test">>)}},

    {ok, SessionSsl} = erlimem:open({tcp, "127.0.0.1", 9999, [ssl]}, erlimem),
    ok = SessionSsl:auth(erlimem, SessionId, GoodCred),
    ?assertEqual(true, is_integer(SessionSsl:run_cmd(login,[]))),
    Bin = list_to_binary(lists:duplicate(1024 * 1024 * 10, 0)), % 10 MB
    ?assertEqual({server_echo, Bin}, SessionSsl:run_cmd(echo,[Bin])),
    ?assertEqual(ok, SessionSsl:close()).