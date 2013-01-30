-module(erlimem).

-include("erlimem.hrl").
-define(SESSMOD, erlimem_session).

%% Application callbacks
-export([start/0, stop/0, open/3, loglevel/1]).

loglevel(L) -> application:set_env(erlimem, logging, L).

start() ->  application:start(?MODULE).
stop()  ->  application:stop(?MODULE).

open(Type, Opts, Cred) ->
    case lists:keymember(erlimem, 1, application:which_applications()) of
    false -> erlimem:start();
    _ -> ok
    end,
    case gen_server:start(?SESSMOD, [Type, Opts, Cred], []) of
        {ok, Pid} -> {?SESSMOD, Pid};
        {error, {error, Error}} -> {error, Error};
        Other -> Other
    end.

% EUnit tests --

-include_lib("eunit/include/eunit.hrl").
-define(Table, test_table_123).

%-define(CONTEST, include).
-ifdef(CONTEST).
%-------------------------------------------------------------------------------------------------------------------
db_conn_test_() ->
    {timeout, 1000000, {
        setup,
        fun setup_con/0,
        fun teardown_con/1,
        {with, [fun logs/1
               , fun all_cons/1
               , fun pswd_process/1
               , fun bad_con_reject/1
        ]}
        }
    }.

setup_con() ->
    io:format(user, "+-----------------------------------------------------------+~n",[]),
    io:format(user, "|                CONNECTION SETUP TESTS                     |~n",[]),
    io:format(user, "+-----------------------------------------------------------+~n",[]),
    application:set_env(imem, mnesia_node_type, ram),
    application:start(imem),
    erlimem:start().

teardown_con(_) ->
    erlimem:stop(),
    application:stop(imem),
    io:format(user, "+===========================================================+~n",[]).

logs(_Sess) ->
    io:format(user, "--------- enable diable change log level (logs) ------------~n",[]),

    erlimem:loglevel(debug),
    ?Debug("This is a debug log"),
    ?Info("This is a info log"),
    ?Error("This is a error log"),

    erlimem:loglevel(info),
    ?Debug("This is should not appear"),
    ?Info("This is a info log"),
    ?Error("This is a error log"),

    erlimem:loglevel(error),
    ?Debug("This is should not appear"),
    ?Info("This is should not appear"),
    ?Error("This is a error log"),

    erlimem:loglevel(disabled),
    ?Debug("This is should not appear"),
    ?Info("This is should not appear"),
    ?Error("This is should not appear"),

    io:format(user, "------------------------------------------------------------~n",[]).

all_cons(_) ->
    io:format(user, "--------- authentication success for tcp/rpc/local ----------~n",[]),
    Schema = 'Imem',
    Cred = {<<"admin">>, erlang:md5(<<"change_on_install">>)},
    ?assertMatch({?SESSMOD, _}, erlimem:open(rpc, {node(), Schema}, Cred)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(tcp, {localhost, 8124, Schema}, Cred)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local_sec, {Schema}, Cred)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local, {Schema}, Cred)),
    io:format(user, "connected successfully~n",[]),
    io:format(user, "------------------------------------------------------------~n",[]).

pswd_process(_) ->
    io:format(user, "----------- driver pswd translate (pswd_process) -----------~n",[]),
    Schema = 'Imem',
    CredMD50 = {<<"admin">>, erlang:md5(<<"change_on_install">>)},
    ?assertMatch({?SESSMOD, _}, erlimem:open(rpc, {node(), Schema}, CredMD50)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(tcp, {localhost, 8124, Schema}, CredMD50)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local_sec, {Schema}, CredMD50)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local, {Schema}, CredMD50)),
    io:format(user, "connected successfully with md5 cred ~p~n",[CredMD50]),
    CredMD51 = {<<"admin">>, <<"change_on_install">>},
    ?assertMatch({?SESSMOD, _}, erlimem:open(rpc, {node(), Schema}, CredMD51)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(tcp, {localhost, 8124, Schema}, CredMD51)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local_sec, {Schema}, CredMD51)),
    ?assertMatch({?SESSMOD, _}, erlimem:open(local, {Schema}, CredMD51)),
    io:format(user, "connected successfully with cleartext cred ~p~n",[CredMD51]),
    io:format(user, "------------------------------------------------------------~n",[]).

bad_con_reject(_) ->
    io:format(user, "--------- authentication failed for rpc/tcp ----------------~n",[]),
    Schema = 'Imem',
    BadCred = {<<"admin">>, erlang:md5(<<"bad password">>)},
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(rpc, {node(), Schema}, BadCred)),
    timer:sleep(1000),
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(tcp, {localhost, 8124, Schema}, BadCred)),
    timer:sleep(1000),
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(local_sec, {Schema}, BadCred)),
    timer:sleep(1000),
    io:format(user, "connections rejected properly~n",[]),
    io:format(user, "------------------------------------------------------------~n",[]).
%-------------------------------------------------------------------------------------------------------------------
-endif.

%-------------------------------------------------------------------------------------------------------------------
db_test_() ->
    {timeout, 1000000, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [fun all_tables/1
                , fun table_create_select_drop/1
                , fun table_modify/1
                , fun simul_insert/1
                , fun table_tail/1
        ]}
        }
    }.

setup(Type) -> 
    User = <<"admin">>,
    Password = erlang:md5(<<"change_on_install">>),
    Cred = {User, Password},
    ImemRunning = lists:keymember(imem, 1, application:which_applications()),
    application:load(imem),
    Schema =
    if ((Type =:= local) orelse (Type =:= local_sec)) andalso (ImemRunning == false) ->
            {ok, S} = application:get_env(imem, mnesia_schema_name),
            S;
        true -> 'Imem'
    end,
    application:set_env(imem, mnesia_node_type, ram),
    application:start(imem),
    erlimem:start(),
    ?Debug("TEST schema ~p", [Schema]),
    case Type of
        tcp         -> erlimem:open(tcp, {localhost, 8124, Schema}, Cred);
        local_sec   -> erlimem:open(local_sec, {Schema}, Cred);
        local       -> erlimem:open(local, {Schema}, Cred)
    end.

setup() ->
    io:format(user, "+-----------------------------------------------------------+~n",[]),
    io:format(user, "|                 TABLE MODIFICATION TESTS                  |~n",[]),
    io:format(user, "+-----------------------------------------------------------+~n",[]),
    erlimem:start(),
    random:seed(erlang:now()),
    setup(local).

teardown(_Sess) ->
   % Sess:close(),
    erlimem:stop(),
    application:stop(imem),
    io:format(user, "+===========================================================+~n",[]).


all_tables(Sess) ->
    io:format(user, "--------- select from all_tables (all_tables) --------------~n",[]),
    Sql = "select name(qname) from all_tables;",
    {ok, Clms, Statement} = Sess:exec(Sql, 100),
    io:format(user, "~p -> ~p~n", [Sql, {Clms, Statement}]),
    Statement:start_async_read(),
    io:format(user, "receiving...~n", []),
    timer:sleep(1000),
    {Rows,_,_} = Statement:next_rows(),
    io:format(user, "received ~p~n", [Rows]),
    Statement:close(),
    io:format(user, "statement closed~n", []),
    io:format(user, "------------------------------------------------------------~n",[]).

table_create_select_drop(Sess) ->
    io:format(user, "-- create insert select drop (table_create_select_drop) ----~n", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 20, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 100),
    io:format(user, "select ~p~n", [{Clms, Statement}]),
    Statement:start_async_read(),
    timer:sleep(1000),
    io:format(user, "receiving...~n", []),
    {Rows,_,_} = Statement:next_rows(),
    Statement:close(),
    io:format(user, "received ~p~n", [length(Rows)]),
    drop_table(Sess, atom_to_list(?Table)),
    io:format(user, "------------------------------------------------------------~n",[]).

table_modify(Sess) ->
    io:format(user, "------- update insert new delete rows (table_modify) -------~n",[]),
    erlimem:loglevel(info),
    create_table(Sess, atom_to_list(?Table)),
    NumRows = 10,
    Res = [Sess:exec("insert into " ++ atom_to_list(?Table) ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');")
          || N <- lists:seq(1,NumRows)],
    ?assertEqual(Res, lists:duplicate(NumRows, ok)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 100),
    io:format(user, "select ~p~n", [{Clms, Statement}]),
    Statement:start_async_read(),
    timer:sleep(1000),
    {Rows,_,_} = Statement:next_rows(),
    io:format(user, "original table from db ~p~n", [Rows]),

    % modify some rows in buffer
    Statement:update_rows([["4",1,4],
                           ["6",10,6],
                           ["8",2,8],
                           ["5",9,5],
                           ["3",4,3]]),
    % delete some rows in buffer
    Statement:delete_rows([["2",6,"6"],
                           ["3",4,"4"],
                           ["4",1,"1"],
                           ["5",9,"9"],
                           ["6",10,"10"],
                           ["7",8,"8"],
                           ["8",2,"2"],
                           ["9",3,"3"]]),

    % insert some rows in buffer
    Statement:insert_rows([[11,11],
                           [12,12]]),

    ok = Statement:prepare_update(),
    ok = Statement:execute_update(),
    ok = Statement:fetch_close(),
    io:format(user, "changed rows!~n", []),

    Statement:start_async_read(),
    timer:sleep(1000),
    {NewRows1,_,_} = Statement:next_rows(),
    ?assertEqual([["1","7","7"],
                  ["2","11","11"],
                  ["3","5","5"],
                  ["4","12","12"]], NewRows1),
    io:format(user, "modified table from db ~p~n", [NewRows1]),
    Statement:close(),
    drop_table(Sess, atom_to_list(?Table)),
    io:format(user, "------------------------------------------------------------~n",[]).

simul_insert(Sess) ->
    io:format(user, "------------ simultaneous insert (simul_insert) ------------~n", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 11, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
    io:format(user, "select ~p~n", [{Clms, Statement}]),
    Statement:start_async_read(),
    timer:sleep(100),
    io:format(user, "receiving sync...~n", []),
    {Rows,_,_} = Statement:next_rows(),
    io:format(user, "received ~p~n", [Rows]),
    io:format(user, "receiving async...~n", []),
    insert_async(Sess, 20, atom_to_list(?Table)),
    ExtraRows = recv_delay(Statement, 10, []),
    io:format(user, "received ~p~n", [ExtraRows]),
    Statement:close(),
    io:format(user, "statement closed~n", []),
    drop_table(Sess, atom_to_list(?Table)),
    io:format(user, "------------------------------------------------------------~n",[]).

table_tail(Sess) ->
    io:format(user, "-------------- fetch async tail (table_tail) ---------------~n", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 10, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
    io:format(user, "select ~p~n", [{Clms, Statement}]),
    Statement:start_async_read(),
    timer:sleep(100),
    io:format(user, "receiving sync...~n", []),
    {Rows,_,_} = Statement:next_rows(),
    io:format(user, "received ~p~n", [Rows]),
    timer:sleep(100),
    Statement:start_async_read([{fetch_mode,push},{tail_mode,true}]),
    io:format(user, "receiving async...~n", []),
    erlimem:loglevel(info),
    insert_async(Sess, 20, atom_to_list(?Table)),
    AsyncRows = recv_delay(Statement, 10, []),
    io:format(user, "received async ~p~n", [AsyncRows]),
    Statement:close(),
    io:format(user, "statement closed~n", []),
    drop_table(Sess, atom_to_list(?Table)),
    io:format(user, "------------------------------------------------------------~n",[]).

create_table(Sess, TableName) ->
    Sql = "create table "++TableName++" (col1 integer, col2 varchar2);",
    Res = Sess:exec(Sql),
    io:format(user, "~p -> ~p~n", [Sql, Res]).

drop_table(Sess, TableName) ->
    Res = Sess:exec("drop table "++TableName++";"),
    io:format(user, "drop table -> ~p~n", [Res]).

recv_delay(_, 0, Rows) -> Rows;
recv_delay(Statement, Count, Rows) ->
    timer:sleep(50),
    {Rs,_,_} = Statement:next_rows(),
    io:format(user, "       received ~p~n", [Rs]),
    recv_delay(Statement, Count-1, Rows ++ Rs).

insert_async(Sess, N, TableName) ->
    F =
    fun
        (_, 0) -> ok;
        (F, Count) ->
            timer:sleep(11),
            Sql = "insert into " ++ TableName ++ " values (" ++ integer_to_list(Count) ++ ", '" ++ integer_to_list(Count) ++ "');",
            Res = Sess:exec(Sql),
            io:format(user, "~p -> ~p~n", [Sql, Res]),
            F(F,Count-1)
    end,
    spawn(fun()-> F(F,N) end).

insert_random(_, 0, Rows) -> Rows;
insert_random(Max, Count, Rows) ->
    Idx = Max + random:uniform(Max),
    insert_random(Max, Count-1, [[Idx, Idx]|Rows]).

update_random(Max, Count, Rows) -> update_random(Max-1, Count, Rows, []).
update_random(_, 0, _, NewRows) -> NewRows;
update_random(Max, Count, Rows, NewRows) ->
    Idx = random:uniform(Max),
    {_, B} = lists:split(Idx-1, Rows),
    [I,PK,_|Rest] = lists:nth(1,B),
    update_random(Max, Count-1, Rows, NewRows ++ [[I,PK,Idx|Rest]]).

insert_range(_Sess, 0, _TableName) -> ok;
insert_range(Sess, N, TableName) when is_integer(N), N > 0 ->
    Sql = "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');",
    Res = Sess:exec(Sql),
    io:format(user, "~p -> ~p~n", [Sql, Res]),
    insert_range(Sess, N-1, TableName).
%-------------------------------------------------------------------------------------------------------------------
