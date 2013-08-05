-module(erlimem).

-include("erlimem.hrl").
-define(SESSMOD, erlimem_session).

-define(LOG(_F),    io:format(user, ?T++" [TEST ~3..0B] "++_F++"~n", [?LINE])).
-define(LOG(_F,_A), io:format(user, ?T++" [TEST ~3..0B] "++_F++"~n", [?LINE]++_A)).

%% Application callbacks
-export([start/0, stop/0, open/3, loglevel/1]).

loglevel(L) -> application:set_env(erlimem, logging, L).

start() ->
    ok = application:load(lager),
    ok = application:set_env(lager, handlers, [{lager_console_backend, info},
                                               {lager_file_backend, [{file, "log/error.log"},
                                                                     {level, error},
                                                                     {size, 10485760},
                                                                     {date, "$D0"},
                                                                     {count, 5}]},
                                               {lager_file_backend, [{file, "log/console.log"},
                                                                     {level, info},
                                                                     {size, 10485760},
                                                                     {date, "$D0"},
                                                                     {count, 5}]}]),
    ok = application:set_env(lager, error_logger_redirect, false),
    ok = application:start(lager),
    application:start(?MODULE).

stop()  ->  application:stop(?MODULE).

open(Type, Opts, Cred) ->
    case lists:keymember(erlimem, 1, application:which_applications()) of
    false -> erlimem:start();
    _ -> ok
    end,
    case gen_server:start(?SESSMOD, [Type, Opts, Cred], []) of
        {ok, Pid} ->
            Sess = {?SESSMOD, Pid},
            IsCorrectVsn = case code:is_loaded(imem_datatype) of
                {file, _} ->
                    LocalVsn = proplists:get_value(vsn,imem_datatype:module_info(attributes)),
                    RemoteVsn = proplists:get_value(vsn, Sess:run_cmd(admin_exec, [imem_datatype,module_info,[attributes]])),
                    if LocalVsn =/= RemoteVsn -> false; true -> true end;
                false -> false
            end,
            case IsCorrectVsn of
                false ->
                    try
                        {imem_datatype,Bin,File} = Sess:run_cmd(admin_exec, [code,get_object_code,[imem_datatype]]),
                        {module, imem_datatype} = code:load_binary(imem_datatype, File, Bin),
                        {ok, {?SESSMOD, Pid}}
                    catch
                        _:Reason ->
                            ?Info("error loading compatible version of imem_datatype ~p", [Reason]),
                            {error, Reason}
                    end;
                true ->
                    {ok, {?SESSMOD, Pid}}
            end;
        {error, {error, Error}} -> {error, Error};
        Other -> Other
    end.

% EUnit tests --
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-define(Table, test_table_123).

-define(RowIdRange(__Rows), [ {from, lists:nth(1, lists:nth(1,__Rows))}
                            , {to, lists:nth(1, lists:nth(length(__Rows),__Rows))}]).

%-define(CONTEST, include).
-ifdef(CONTEST).
%-------------------------------------------------------------------------------------------------------------------
db_conn_test_() ->
    {timeout, 1000000, {
        setup,
        fun setup_con/0,
        fun teardown_con/1,
        {with, [fun logs/1
               , fun row_fun/1
               %, fun all_cons/1
               %, fun pswd_process/1
               %, fun bad_con_reject/1
        ]}
        }
    }.

row_fun(_) ->
    ?LOG("---------- checking generated row_fun (row_fun) ------------"),
    Schema = 'Imem',
    Cred = {<<"admin">>, erlang:md5(<<"change_on_install">>)},
    {_, SessLocal} = erlimem:open(tcp, {localhost, 8124, Schema}, Cred),
    {_, SessMpro} = erlimem:open(tcp, {localhost, 8125, Schema}, Cred),
    RF = SessLocal:run_cmd(select_rowfun_str, [[#ddColMap{type=string,tind=1,cind=2}], eu, undefined, undefined]),
    RF1 = SessMpro:run_cmd(select_rowfun_str, [[#ddColMap{type=integer,tind=1,cind=2}], eu, undefined, undefined]),
    RF2 = SessMpro:run_cmd(select_rowfun_str, [[#ddColMap{type=string,tind=1,cind=3}], eu, undefined, undefined]),
    ?assert(is_function(RF)), 
    ?assert(is_function(RF1)),
    ?LOG("rowfun local ~p, remote ~p, another remote ~p", [RF, RF1, RF2]),
    ?assertEqual(RF, RF1),
    ?assertEqual(["6"],RF({{dummy,"6"},{}})),
    ?LOG("rowfun local executed"),
    ?assertEqual(["5"],RF1({{dummy,5},{}})), 
    ?LOG("rowfun remote executed"),
    timer:sleep(1000),
    %% SessLocal:close(),
    %% SessMpro:close(),
    ?LOG("------------------------------------------------------------").

setup_con() ->
    ?LOG("+-----------------------------------------------------------+"),
    ?LOG("|                CONNECTION SETUP TESTS                     |"),
    ?LOG("+-----------------------------------------------------------+"),
    application:set_env(imem, mnesia_node_type, ram),
    application:start(imem). 

teardown_con(_) ->
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
                %, fun all_tables/1
                %, fun table_create_select_navigate_drop/1
                %, fun table_sort_navigate/1
                %, fun table_modify/1
                %, fun simul_insert/1
                %, fun table_tail/1
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
    random:seed(erlang:now()),
    setup(tcp).

teardown(_Sess) ->
   % Sess:close(),
    application:stop(imem),
    ?LOG("+===========================================================+").

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
    DataTypes = [timestamp, atom, ipaddr, integer, ipaddr,integer, binary, term],
    Result = Sess:run_cmd(create_table, [TableName
                               , {Fields, DataTypes, list_to_tuple([TableName]++Fields)}
                               , [{record_name, TableName},{type, ordered_set}]]),
    %% We are testing a error here.
    ?assertMatch({error, _}, Result),
    ?LOG("------------------------------------------------------------", []).

% - all_tables({ok, Sess}) ->
% -     ?LOG("--------- select from all_tables (all_tables) --------------"),
% -     Sql = "select name(qname) from all_tables;",
% -     {ok, Clms, Statement} = Sess:exec(Sql, 100),
% -     ?LOG("~p ->~n ~p", [Sql, {Clms, Statement}]),
% -     Statement:gui_req(button, <<">|">>, fun(GReq) ->
% -         ?LOG("received ~p", [GReq#gres.rows]),
% -         Statement:gui_req(button, <<"close">>, fun(_GReq) ->
% -             ?LOG("statement ~p closed", [Statement])
% -         end)
% -     end),
% -     timer:sleep(500),
% -     ?LOG("------------------------------------------------------------").
% - 
% - table_create_select_navigate_drop({ok, Sess}) ->
% -     ?LOG("------- navigate (table_create_select_navigate_drop) -------", []),
% -     create_table(Sess, atom_to_list(?Table)),
% -     insert_range(Sess, 200, atom_to_list(?Table)),
% -     {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
% -     ?LOG("select ~p", [{Clms, Statement}]),
% - 
% -     Statement:gui_req(button, <<">">>, fun(GReq) ->
% -         ?LOG("rows ~p", [GReq#gres.rows]),
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 1}, {to, 10}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, <<">">>, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 11}, {to, 20}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, <<">>">>, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 31}, {to, 40}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, <<"<<">>, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 16}, {to, 25}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, <<">">>, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 26}, {to, 35}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, <<"|<">>, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 1}, {to, 10}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     Statement:gui_req(button, 25, fun(GReq) ->
% -         ?assert(length(GReq#gres.rows) > 0),
% -         ?assertEqual([{from, 16}, {to, 25}], ?RowIdRange(GReq#gres.rows))
% -     end),
% - 
% -     ?LOG(">  Read rows from  1 to 10", []),
% -     ?LOG(">  Read rows from 10 to 20", []),
% -     ?LOG(">> Read rows from 31 to 40", []),
% -     ?LOG("<< Read rows from 16 to 25", []),
% -     ?LOG(">  Read rows from 26 to 35", []),
% -     ?LOG("|< Read rows from  1 to 10", []),
% -     ?LOG("@ 25 Read 16 row from 25", []),
% - 
% -     Statement:gui_req(button, <<"close">>, fun(_GReq) -> ok end),
% -     drop_table(Sess, atom_to_list(?Table)),
% -     ?LOG("------------------------------------------------------------").
% - 
% - table_sort_navigate({ok, Sess}) ->
% -     ?LOG("------- sorted table navigate (table_sort_navigate) --------", []),
% -     create_table(Sess, atom_to_list(?Table)),
% -     insert_range(Sess, 200, atom_to_list(?Table)),
% -     {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++" order by col2;", 10),
% -     ?LOG("select ~p", [{Clms, Statement}]),
% - 
% -     Rows = Statement:gui_req(button, <<">">>),
% -     ?assert(length(Rows#gres.rows) > 0),
% -     ?assertEqual([{from, 4}, {to, 3}], ?RowIdRange(Rows#gres.rows)),
% - 
% -     Rows0 = Statement:gui_req(button, <<">">>),
% -     ?assert(length(Rows0#gres.rows) > 0),
% -     ?assertEqual([{from, 17}, {to, 36}], ?RowIdRange(Rows0#gres.rows)),
% - 
% -     Rows1 = Statement:gui_req(button, <<">>">>),
% -     ?assert(length(Rows#gres.rows) > 0),
% -     ?assertEqual([{from, 159}, {to, 157}], ?RowIdRange(Rows1#gres.rows)),
% - 
% -     Rows2 = Statement:gui_req(button, <<"<<">>),
% -     ?assert(length(Rows2#gres.rows) > 0),
% -     ?assertEqual([{from, 89}, {to, 154}], ?RowIdRange(Rows2#gres.rows)),
% - 
% -     Rows3 = Statement:gui_req(button, <<">">>),
% -     ?assert(length(Rows3#gres.rows) > 0),
% -     ?assertEqual([{from, 181}, {to, 179}], ?RowIdRange(Rows3#gres.rows)),
% - 
% -     Rows4 = Statement:gui_req(button, <<"|<">>),
% -     ?assert(length(Rows4#gres.rows) > 0),
% -     ?assertEqual([{from, 136}, {to, 72}], ?RowIdRange(Rows4#gres.rows)),
% - 
% -     Rows5 = Statement:gui_req(button, 25),
% -     ?assert(length(Rows5#gres.rows) > 0),
% -     ?assertEqual([{from, 94}, {to, 44}], ?RowIdRange(Rows5#gres.rows)),
% - 
% -     ?LOG(">  Read rows from   4 to 3", []),
% -     ?LOG(">  Read rows from  17 to 36", []),
% -     ?LOG(">> Read rows from 159 to 157", []),
% -     ?LOG("<< Read rows from  89 to 154", []),
% -     ?LOG(">  Read rows from 181 to 179", []),
% -     ?LOG("|< Read rows from 136 to 72", []),
% -     ?LOG("@ 25 Read 94 row from 44", []),
% - 
% -     Statement:gui_req(button, <<"close">>),
% -     drop_table(Sess, atom_to_list(?Table)),
% -     ?LOG("------------------------------------------------------------").
% - 
% - table_modify({ok, Sess}) ->
% -     ?LOG("------- update insert new delete rows (table_modify) -------"),
% -     erlimem:loglevel(info),
% -     create_table(Sess, atom_to_list(?Table)),
% -     NumRows = 10,
% -     Res = [Sess:exec("insert into " ++ atom_to_list(?Table) ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');")
% -           || N <- lists:seq(1,NumRows)],
% -     ?assertEqual(Res, lists:duplicate(NumRows, ok)),
% -     {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 100),
% -     ?LOG("select ~p", [{Clms, Statement}]),
% - 
% -     Rows = Statement:gui_req(button, <<">|">>),
% -     ?assert(length(Rows#gres.rows) > 0),
% -     ?assertEqual([{from, 1}, {to, 10}], ?RowIdRange(Rows#gres.rows)),
% -     ?LOG("original table from db ~p", [Rows#gres.rows]),
% - 
% -     %% % modify some rows in buffer
% -     %% Statement:update_rows([["4",1,4],
% -     %%                        ["6",10,6],
% -     %%                        ["8",2,8],
% -     %%                        ["5",9,5],
% -     %%                        ["3",4,3]]),
% -     %% % delete some rows in buffer
% -     %% Statement:delete_rows([["2",6,"6"],
% -     %%                        ["3",4,"4"],
% -     %%                        ["4",1,"1"],
% -     %%                        ["5",9,"9"],
% -     %%                        ["6",10,"10"],
% -     %%                        ["7",8,"8"],
% -     %%                        ["8",2,"2"],
% -     %%                        ["9",3,"3"]]),
% - 
% -     %% % insert some rows in buffer
% -     %% Statement:insert_rows([[11,11],
% -     %%                        [12,12]]),
% - 
% -     %% ok = Statement:prepare_update(),
% -     %% ok = Statement:execute_update(),
% -     %% ok = Statement:fetch_close(),
% -     %% ?LOG("changed rows!", []),
% - 
% -     %% Statement:start_async_read([]),
% -     %% timer:sleep(100),
% -     %% {NewRows1,_,_} = Statement:next_rows(),
% -     %% ?assertEqual([["1","7","7"],
% -     %%               ["2","11","11"],
% -     %%               ["3","5","5"],
% -     %%               ["4","12","12"]], NewRows1),
% -     %% ?LOG("modified table from db ~p", [NewRows1]),
% - 
% -     Statement:gui_req(button, <<"close">>),
% -     drop_table(Sess, atom_to_list(?Table)),
% -     ?LOG("------------------------------------------------------------").
% - 
% - simul_insert({ok, Sess}) ->
% -     ?LOG("------------ simultaneous insert (simul_insert) ------------", []),
% -     create_table(Sess, atom_to_list(?Table)),
% -     insert_range(Sess, 21, atom_to_list(?Table)),
% -     {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 10),
% -     ?LOG("select ~p", [{Clms, Statement}]),
% - 
% -     Rows = Statement:gui_req(button, <<">">>),
% -     ?LOG("received ~p", [Rows#gres.rows]),
% - 
% -     ?LOG("receiving async...", []),
% -     insert_async(Sess, 20, atom_to_list(?Table)),
% -     ExtraRows = recv_delay(Statement, <<">">>, 10, []),
% -     ?LOG("received ~p", [ExtraRows]),
% - 
% -     Statement:gui_req(button, <<"close">>),
% -     drop_table(Sess, atom_to_list(?Table)),
% -     ?LOG("------------------------------------------------------------").
% - 
% - table_tail({ok, Sess}) ->
% -     ?LOG("-------------- fetch async tail (table_tail) ---------------", []),
% -     create_table(Sess, atom_to_list(?Table)),
% -     insert_range(Sess, 16, atom_to_list(?Table)),
% -     {ok, Clms, Statement} = Sess:exec("select * from "++atom_to_list(?Table)++";", 5),
% -     ?LOG("select ~p", [{Clms, Statement}]),
% - 
% -     Rows = Statement:gui_req(button, <<">">>),
% -     Rows0 = Statement:gui_req(button, <<">|...">>),
% - 
% -     [{from, Frm}, {to, To}] = ?RowIdRange(Rows#gres.rows),
% -     ?LOG(">     Read rows from ~p to ~p", [Frm, To]),
% -     [{from, Frm0}, {to, To0}] = ?RowIdRange(Rows0#gres.rows),
% -     ?LOG(">|... Read rows from ~p to ~p", [Frm0, To0]),
% - 
% -     ?LOG("receiving async...", []),
% -     insert_async(Sess, 20, atom_to_list(?Table)),
% -     AsyncRows = recv_delay(Statement, Rows0#gres.loop, 10, []),
% - 
% -     [[Frm1|_]|_] = AsyncRows,
% -     [To1|_] = lists:last(AsyncRows),
% -     ?LOG("received async from ~p to ~p", [Frm1, To1]),
% -     ?assertEqual(20, length(AsyncRows)),
% - 
% -     Statement:gui_req(button, <<"close">>),
% -     drop_table(Sess, atom_to_list(?Table)),
% -     ?LOG("------------------------------------------------------------").
% - 
% - create_table(Sess, TableName) ->
% -     Sql = "create table "++TableName++" (
% -                      col2 integer,
% -                      col1 varchar2(10)
% -           );",
% -     Res = Sess:exec(Sql),
% -     ?LOG("~p -> ~p", [Sql, Res]).
% - 
% - drop_table(Sess, TableName) ->
% -     Res = Sess:exec("drop table "++TableName++";"),
% -     ?LOG("drop table -> ~p", [Res]).
% - 
% - recv_delay(_, _, 0, Rows) -> Rows;
% - recv_delay(Statement, Btn, Count, Rows) ->
% -     timer:sleep(50),
% -     if byte_size(Btn) > 0 ->
% -         Rs = Statement:gui_req(button, Btn),
% -         if Rs#gres.rows =/= [] ->
% -             [{from, Frm}, {to, To}] = ?RowIdRange(Rs#gres.rows),
% -             ?LOG("loop ~p read from ~p to ~p", [list_to_atom(binary_to_list(Btn)), Frm, To]);
% -         true -> ok end,
% -         if Rs#gres.loop =/= undefined ->
% -             recv_delay(Statement, Rs#gres.loop, Count-1, Rows ++ Rs#gres.rows);
% -             true -> if Btn =/= undefined ->
% -                         recv_delay(Statement, Btn, Count-1, Rows ++ Rs#gres.rows);
% -                         true -> Rows
% -                     end
% -         end;
% -     true -> Rows
% -     end.
% - 
% - insert_async(Sess, N, TableName) ->
% -     F =
% -     fun
% -         (_, 0) -> ok;
% -         (F, Count) ->
% -             timer:sleep(11),
% -             Sql = "insert into " ++ TableName ++ " values (" ++ integer_to_list(Count) ++ ", '" ++ integer_to_list(Count) ++ "');",
% -             Res = Sess:exec(Sql),
% -             ?LOG("~p -> ~p", [Sql, Res]),
% -             F(F,Count-1)
% -     end,
% -     spawn(fun()-> F(F,N) end).
% - 
%% insert_random(_, 0, Rows) -> Rows;
%% insert_random(Max, Count, Rows) ->
%%     Idx = Max + random:uniform(Max),
%%     insert_random(Max, Count-1, [[Idx, Idx]|Rows]).
%% 
%% update_random(Max, Count, Rows) -> update_random(Max-1, Count, Rows, []).
%% update_random(_, 0, _, NewRows) -> NewRows;
%% update_random(Max, Count, Rows, NewRows) ->
%%     Idx = random:uniform(Max),
%%     {_, B} = lists:split(Idx-1, Rows),
%%     [I,PK,_|Rest] = lists:nth(1,B),
%%     update_random(Max, Count-1, Rows, NewRows ++ [[I,PK,Idx|Rest]]).

% insert_range(S, N, T) ->
%     io:format(user, "inserting.... [", []),
%     ins_range(S, N, T).
% 
% ins_range(_Sess, 0, _TableName) -> io:format(user, "]~n", []);
% ins_range(Sess, N, TableName) when is_integer(N), N > 0 ->
%     Sql = "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');",
%     _Res = Sess:exec(Sql),
%     %?LOG("~p -> ~p", [Sql, _Res]),
%     io:format(user, "~p ", [N]),
%     ins_range(Sess, N-1, TableName).
%-------------------------------------------------------------------------------------------------------------------

-endif.
-endif.
