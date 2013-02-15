-module(erlimem).

-include("erlimem.hrl").
-define(SESSMOD, erlimem_session).

-define(LOG(_F),    io:format(user, "[TEST] "++_F++"~n", [])).
-define(LOG(_F,_A), io:format(user, "[TEST] "++_F++"~n", _A)).

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
        {ok, Pid} -> {ok, {?SESSMOD, Pid}};
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
    ?LOG("+-----------------------------------------------------------+"),
    ?LOG("|                CONNECTION SETUP TESTS                     |"),
    ?LOG("+-----------------------------------------------------------+"),
    application:set_env(imem, mnesia_node_type, ram),
    application:start(imem),
    erlimem:start().

teardown_con(_) ->
    erlimem:stop(),
    application:stop(imem),
    ?LOG("+===========================================================+").

logs(_Sess) ->
    ?LOG("--------- enable diable change log level (logs) ------------"),

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

    ?LOG("------------------------------------------------------------").

all_cons(_) ->
    ?LOG("--------- authentication success for tcp/rpc/local ----------"),
    Schema = 'Imem',
    Cred = {<<"admin">>, erlang:md5(<<"change_on_install">>)},
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(rpc, {node(), Schema}, Cred)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(tcp, {localhost, 8124, Schema}, Cred)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local_sec, {Schema}, Cred)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local, {Schema}, Cred)),
    ?LOG("connected successfully"),
    ?LOG("------------------------------------------------------------").

pswd_process(_) ->
    ?LOG("----------- driver pswd translate (pswd_process) -----------"),
    Schema = 'Imem',
    CredMD50 = {<<"admin">>, erlang:md5(<<"change_on_install">>)},
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(rpc, {node(), Schema}, CredMD50)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(tcp, {localhost, 8124, Schema}, CredMD50)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local_sec, {Schema}, CredMD50)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local, {Schema}, CredMD50)),
    ?LOG("connected successfully with md5 cred ~p",[CredMD50]),
    CredMD51 = {<<"admin">>, <<"change_on_install">>},
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(rpc, {node(), Schema}, CredMD51)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(tcp, {localhost, 8124, Schema}, CredMD51)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local_sec, {Schema}, CredMD51)),
    ?assertMatch({ok, {?SESSMOD, _}}, erlimem:open(local, {Schema}, CredMD51)),
    ?LOG("connected successfully with cleartext cred ~p",[CredMD51]),
    ?LOG("------------------------------------------------------------").

bad_con_reject(_) ->
    ?LOG("--------- authentication failed for rpc/tcp ----------------"),
    Schema = 'Imem',
    BadCred = {<<"admin">>, erlang:md5(<<"bad password">>)},
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(rpc, {node(), Schema}, BadCred)),
    timer:sleep(1000),
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(tcp, {localhost, 8124, Schema}, BadCred)),
    timer:sleep(1000),
    ?assertMatch({error,{'SecurityException',{_,_}}}, erlimem:open(local_sec, {Schema}, BadCred)),
    timer:sleep(1000),
    ?LOG("connections rejected properly"),
    ?LOG("------------------------------------------------------------").
%-------------------------------------------------------------------------------------------------------------------

-else.

%-------------------------------------------------------------------------------------------------------------------
db_test_() ->
    {timeout, 1000000, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [fun native_apis/1
                , fun all_tables/1
                , fun table_create_select_drop/1
                , fun table_modify/1
                , fun simul_insert/1
                , fun table_no_eot/1
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
    ?LOG("+-----------------------------------------------------------+"),
    ?LOG("|                 TABLE MODIFICATION TESTS                  |"),
    ?LOG("+-----------------------------------------------------------+"),
    erlimem:start(),
    random:seed(erlang:now()),
    setup(local).

teardown(_Sess) ->
   % Sess:close(),
    erlimem:stop(),
    application:stop(imem),
    ?LOG("+===========================================================+").

all_tables({ok, Sess}) ->
    ?LOG("--------- select from all_tables (all_tables) --------------"),
    Sql = "select name(qname) from all_tables;",
    {ok, Clms, Statement} = Sess:exec(Sql, 100),
    ?LOG("~p -> ~p", [Sql, {Clms, Statement}]),
    Statement:start_async_read([]),
    ?LOG("receiving...", []),
    timer:sleep(1000),
    {Rows,_,_} = Statement:next_rows(),
    ?LOG("received ~p", [Rows]),
    Statement:close(),
    ?LOG("statement closed", []),
    ?LOG("------------------------------------------------------------").

native_apis({ok, Sess}) ->
    ?LOG("---------- native API test (table_native_create) -----------", []),
    TableName = 'smpp@',
    Fields = [time             
             , protocol         
             , level            
             , originator_addr  
             , originator_port  
             , destination_addr 
             , destination_port 
             , pdu              
             , extra],
    SchemaName = Sess:run_cmd(schema, []),
    DataTypes = [timestamp, atom, ipaddr, integer, ipaddr,integer, binary, term],
    Sess:run_cmd(create_table, [TableName
                               , {Fields, DataTypes, list_to_tuple([TableName]++Fields)}
                               , [{record_name, TableName},{type, ordered_set}]
                               , SchemaName]),
    ?LOG("------------------------------------------------------------", []).

table_create_select_drop({ok, Sess}) ->
    ?LOG("-- create insert select drop (table_create_select_drop) ----", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 20, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 100),
    ?LOG("select ~p", [{Clms, Statement}]),
    Statement:start_async_read([]),
    timer:sleep(1000),
    ?LOG("receiving...", []),
    {Rows,_,_} = Statement:next_rows(),
    Statement:close(),
    ?LOG("received ~p", [length(Rows)]),
    drop_table(Sess, atom_to_list(?Table)),
    ?LOG("------------------------------------------------------------").

table_modify({ok, Sess}) ->
    ?LOG("------- update insert new delete rows (table_modify) -------"),
    erlimem:loglevel(info),
    create_table(Sess, atom_to_list(?Table)),
    NumRows = 10,
    Res = [Sess:exec("insert into " ++ atom_to_list(?Table) ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');")
          || N <- lists:seq(1,NumRows)],
    ?assertEqual(Res, lists:duplicate(NumRows, ok)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 100),
    ?LOG("select ~p", [{Clms, Statement}]),
    Statement:start_async_read([]),
    timer:sleep(1000),
    {Rows,_,_} = Statement:next_rows(),
    ?LOG("original table from db ~p", [Rows]),

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
    ?LOG("changed rows!", []),

    Statement:start_async_read(),
    timer:sleep(1000),
    {NewRows1,_,_} = Statement:next_rows(),
    ?assertEqual([["1","7","7"],
                  ["2","11","11"],
                  ["3","5","5"],
                  ["4","12","12"]], NewRows1),
    ?LOG("modified table from db ~p", [NewRows1]),
    Statement:close(),
    drop_table(Sess, atom_to_list(?Table)),
    ?LOG("------------------------------------------------------------").

simul_insert({ok, Sess}) ->
    ?LOG("------------ simultaneous insert (simul_insert) ------------", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 11, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
    ?LOG("select ~p", [{Clms, Statement}]),
    Statement:start_async_read([]),
    timer:sleep(100),
    ?LOG("receiving sync...", []),
    {Rows,_,_} = Statement:next_rows(),
    ?LOG("received ~p", [Rows]),
    ?LOG("receiving async...", []),
    insert_async(Sess, 20, atom_to_list(?Table)),
    ExtraRows = recv_delay(Statement, 10, []),
    ?LOG("received ~p", [ExtraRows]),
    Statement:close(),
    ?LOG("statement closed", []),
    drop_table(Sess, atom_to_list(?Table)),
    ?LOG("------------------------------------------------------------").

table_no_eot({ok, Sess}) ->
    ?LOG("----------------- fetch all (table_no_eot) ----------------", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 10, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
    ?LOG("select ~p", [{Clms, Statement}]),
    Statement:start_async_read([]),
    timer:sleep(100),
    ?LOG("receiving sync...", []),
    {Rows,_,_} = Statement:next_rows(),
    ?LOG("received ~p", [Rows]),
    drop_table(Sess, atom_to_list(?Table)),
    ?LOG("------------------------------------------------------------").

table_tail({ok, Sess}) ->
    ?LOG("-------------- fetch async tail (table_tail) ---------------", []),
    create_table(Sess, atom_to_list(?Table)),
    insert_range(Sess, 16, atom_to_list(?Table)),
    {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 5),
    ?LOG("select ~p", [{Clms, Statement}]),
    Statement:start_async_read([]),
    timer:sleep(100),
    ?LOG("receiving sync...", []),
    {Rows,_,_} = Statement:next_rows(),
    ?LOG("received ~p", [Rows]),
    timer:sleep(100),
    Statement:start_async_read([{fetch_mode,push},{tail_mode,true}]),
    ?LOG("receiving async...", []),
    erlimem:loglevel(info),
    insert_async(Sess, 20, atom_to_list(?Table)),
    AsyncRows = recv_delay(Statement, 10, []),
    ?LOG("received async ~p", [AsyncRows]),
    ?assertEqual(36, length(AsyncRows)),
    Statement:close(),
    ?LOG("statement closed", []),
    drop_table(Sess, atom_to_list(?Table)),
    ?LOG("------------------------------------------------------------").

create_table(Sess, TableName) ->
    Sql = "create table "++TableName++" (col1 integer, col2 varchar2);",
    Res = Sess:exec(Sql),
    ?LOG("~p -> ~p", [Sql, Res]).

drop_table(Sess, TableName) ->
    Res = Sess:exec("drop table "++TableName++";"),
    ?LOG("drop table -> ~p", [Res]).

recv_delay(_, 0, Rows) -> Rows;
recv_delay(Statement, Count, Rows) ->
    timer:sleep(50),
    {Rs,_,_} = Statement:next_rows(),
    ?LOG("       received ~p", [Rs]),
    recv_delay(Statement, Count-1, Rows ++ Rs).

insert_async(Sess, N, TableName) ->
    F =
    fun
        (_, 0) -> ok;
        (F, Count) ->
            timer:sleep(11),
            Sql = "insert into " ++ TableName ++ " values (" ++ integer_to_list(Count) ++ ", '" ++ integer_to_list(Count) ++ "');",
            Res = Sess:exec(Sql),
            ?LOG("~p -> ~p", [Sql, Res]),
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
    ?LOG("~p -> ~p", [Sql, Res]),
    insert_range(Sess, N-1, TableName).
%-------------------------------------------------------------------------------------------------------------------

-endif.

