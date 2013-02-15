-module(erlimem_buf).

-include("erlimem.hrl").

-record(buffer,
    { row_top = 0
    , row_bottom = 0
    , tableid
    , rowfun
    }).

-export([create/1
        , clear/1
        , delete/1
        , get_rows_from_ets/1
        , insert_rows/2
        , modify_rows/3
        , get_modified_rows/1
        , row_with_key/2
        , get_prev_rows/2
        , get_rows_from/3
        , get_next_rows/2
        , get_buffer_max/1
        , rfun/1
        ]).

create(RowFun) ->
    #buffer{tableid=ets:new(results, [ordered_set, public])
           , rowfun = RowFun
    }.

clear(#buffer{tableid=Tab} = Buf) ->
    ?Debug("clearing buffer ~p", [Tab]),
    true = ets:delete_all_objects(Tab),
    Buf#buffer{row_top=0,row_bottom=0}.

delete(#buffer{tableid=Tab}) -> true = ets:delete(Tab).

row_with_key(#buffer{tableid=TableId}, RowNum) ->
    ets:lookup(TableId, RowNum).

get_modified_rows(#buffer{tableid=TableId}) ->
    [tuple_to_list(R) || R <- ets:select(TableId,[{'$1',[{'=/=',nop,{element,2,'$1'}}],['$_']}])].

modify_rows(Buf, ins, Rows) -> insert_new_rows(Buf, Rows);
modify_rows(#buffer{tableid=TableId}, Op, Rows) when is_atom(Op) ->
    ExistingRows = [ets:lookup(TableId, list_to_integer(I)) || [I|_] <- Rows],
    NewRows = apply_op(Op, ExistingRows, Rows, []),
    ?Debug("modify_rows from ~p~nto ~p~nwith ~p", [ExistingRows,NewRows,Rows]),
    ets:insert(TableId, [list_to_tuple(R)||R<-NewRows]).

apply_op(Op, [_|_] = ExistingRows, [F|_] = NewRows, []) when is_list(F)->
    apply_op(Op, ExistingRows, [list_to_tuple([list_to_integer(I)|R]) || [I|R]<-NewRows], []);
apply_op(_,[],[],ModifiedRows) -> ModifiedRows;
apply_op(Op,[[Er|_] | ExistingRows],NewRows,ModifiedRows) when is_atom(Op) ->
    {[I,_,K], R} = case lists:split(3, tuple_to_list(Er)) of
        {[_,ins,_], _} = D -> NewOp = ins, D;
        D ->              NewOp = Op, D
    end,
    {NewR, NewRows0} = case lists:keytake(I,1,NewRows) of
        {value, V, NRs0} -> {[I, NewOp, K | lists:nthtail(1,tuple_to_list(V))], NRs0};
        false -> {[I, NewOp, K | R], NewRows}
    end,
    apply_op(Op, ExistingRows, NewRows0, ModifiedRows ++ [NewR]).

insert_rows(#buffer{tableid=TableId}, Rows) ->
    NrOfRows = length(Rows),
    CacheSize = ets:info(TableId, size),
    ets:insert(TableId, [list_to_tuple([I,nop|[R]])||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), Rows)]).

insert_new_rows(#buffer{tableid=TableId}, Rows) ->
    NrOfRows = length(Rows),
    CacheSize = ets:info(TableId, size),
    ets:insert(TableId, [list_to_tuple([I,ins,{}|R])||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), Rows)]).

% TODO - complete state of the buffer (true/false) need to be determined
get_rows_from_ets(#buffer{row_top=RowStart0, row_bottom=RowEnd, rowfun=F, tableid=TableId}) ->
    CacheSize = ets:info(TableId, size),
    RowStart = if RowStart0 =:= 0 -> 1; true -> RowStart0 end, % ETS tables are indexed from 1
    case ets:lookup(TableId, RowStart) of
        [FirstRow] ->
            {MatchHead, MatchExpr} = build_match(size(FirstRow)),
            Rows = ets:select(TableId,[{MatchHead,[{'>=','$1',RowStart},{'=<','$1',RowEnd}],[MatchExpr]}]),
            ?Debug("get_rows_from_ets selected rows (~p,~p) ~p", [RowStart, RowEnd, Rows]),
            NewRows = lists:foldl(  fun
                                        ([I,Op,RK],Rws) when is_integer(I) ->
                                            Row = F(RK),
                                            ets:insert(TableId, list_to_tuple([I, Op, RK | Row])),
                                            Rws ++ [[integer_to_list(I)|Row]];
                                        ([I,_Op,_RK|Rest],Rws) when is_integer(I) ->
                                            ?Debug("get_rows_from_ets no insert ~p", [[_RK|Rest]]),
                                            Rws ++ [[integer_to_list(I)|Rest]]
                                    end
                                    , []
                                    , Rows
                                    ),
            {NewRows, true, CacheSize};
        [] ->
            ?Info("get_rows_from_ets _NO_ROWS_ from ~p of total ~p rows", [RowStart, CacheSize]),
            {[], true, CacheSize}
    end.

rfun(R) -> R.
    
build_match(Count) -> build_match(Count, {[],[]}).
build_match(0, {Head,Expr}) -> {list_to_tuple(Head),Expr};
build_match(Count, {Head,Expr}) ->
    MVar = list_to_atom("$" ++ integer_to_list(Count)),
    build_match(Count-1, {[MVar|Head], [MVar|Expr]}).
    
% TODO - complete state of the buffer (true/false) need to be determined
get_buffer_max(#buffer{tableid=TableId}) ->
    {ok, ets:info(TableId, size)}.

get_rows_from(#buffer{tableid=TableId} = Buf, RowNum, MaxRows) ->
    CacheSize = ets:info(TableId, size),
    NewRowTop =
        if RowNum > CacheSize -> CacheSize;
           true -> RowNum
        end,
    NewRowBottom =
        if (NewRowTop + MaxRows - 1) > CacheSize -> CacheSize;
           true -> (NewRowTop + MaxRows - 1)
        end,
    NewBuf = Buf#buffer{row_top=NewRowTop,row_bottom=NewRowBottom},
    ?Debug("get_rows_from from ~p to ~p of total ~p rows", [NewRowTop, NewRowBottom, CacheSize]),
    {get_rows_from_ets(NewBuf), NewBuf}.

get_prev_rows(#buffer{row_top=RowTop,tableid=TableId} = Buf, MaxRows) ->
    _CacheSize = ets:info(TableId, size),
    NewRowTop =
        if (RowTop - MaxRows) < 1 -> 1;
           true -> (RowTop - MaxRows)
        end,
    NewRowBottom =
        if (RowTop - 1) < 1 -> 1;
            true -> (RowTop - 1)
        end,
    NewBuf = Buf#buffer{row_top=NewRowTop,row_bottom=NewRowBottom},
    if (RowTop == NewRowTop) ->
            ?Info("get_prev_rows _NO_ROWS_ from ~p to ~p of total ~p rows", [NewRowTop, NewRowBottom, _CacheSize]),
            {{[],undefined,undefined}, NewBuf};
        true ->
            ?Debug("get_prev_rows from ~p to ~p of total ~p rows", [NewRowTop, NewRowBottom, _CacheSize]),
            {_Rows,_,_} = Result = get_rows_from_ets(NewBuf),
            ?Debug("Rows ~p OldBuf ~p NewBuf ~p", [length(_Rows), Buf, NewBuf]),
            {Result, NewBuf}
    end.

get_next_rows(#buffer{row_bottom=RowBottom, tableid=TableId} = Buf, MaxRows) ->
    CacheSize = ets:info(TableId, size),
    NewRowBottom =
        if (RowBottom + MaxRows) > CacheSize -> CacheSize;
            true -> (RowBottom + MaxRows)
        end,
    NewRowTop =
        if (RowBottom + 1) > CacheSize -> CacheSize;
            true -> (RowBottom + 1)
        end,
    NewBuf = Buf#buffer{row_top=NewRowTop,row_bottom=NewRowBottom},
    if (RowBottom == NewRowBottom) ->
            ?Info("get_next_rows _NO_ROWS_ from ~p to ~p of total ~p rows", [NewRowTop, NewRowBottom, CacheSize]),
            {{[],undefined,undefined}, NewBuf};
        true ->
            ?Debug("get_next_rows from ~p to ~p of total ~p rows", [NewRowTop, NewRowBottom, CacheSize]),
            {_Rows,_,_} = Result = get_rows_from_ets(NewBuf),
            ?Debug("Rows ~p OldBuf ~p NewBuf ~p", [length(_Rows), Buf, NewBuf]),
            {Result, NewBuf}
    end.

% EUnit tests --

-include_lib("eunit/include/eunit.hrl").

setup() -> ok.
teardown(_) -> ok.

db_test_() ->
    {timeout, 100000, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            %fun read_test/1
             fun read_test_rwfun/1
             , fun update_test_rwfun/1
        ]}
        }
    }.

read_test_rwfun(_) ->
    io:format(user, "---- read fun test ----~n", []),
    Buf = create(fun(R)->
                            lists:nthtail(1,tuple_to_list(R))
                        end),
    RowCount = 9,
    ok = insert_tuples(RowCount, Buf),
    io:format(user, "inserted ~p rows~n", [RowCount]),
    {Rows, NewBuf} = read_all_fwd(Buf, 3, []),
    io:format(user, "read forward ~p~n", [Rows]),
    {NewRows, NewBuf1} = read_all_bk(NewBuf, 3, []),
    io:format(user, "read backward ~p~n", [NewRows]),
    {{NewRows1,_,_}, NewBuf2} = get_rows_from(NewBuf1, 3, 9),
    io:format(user, "read middle-end ~p~n", [NewRows1]),
    {{NewRows2,_,_}, _} = get_rows_from(NewBuf2, 1, 4),
    io:format(user, "read middle-start ~p~n", [NewRows2]),
    delete(Buf),
    io:format(user, "-------------------~n", []).

update_test_rwfun(_) ->
    io:format(user, "---- update test ----~n", []),
    Buf = create(fun(R)->
                            lists:nthtail(1,tuple_to_list(R))
                        end),
    insert_rows(Buf, [
            [{table, 11,12,13}]
        ,   [{table, 21,22,23}]
        ,   [{table, 31,32,33}]
        ,   [{table, 41,42,43}]
        ,   [{table, 51,52,53}]
    ]),
    io:format(user, "inserted some rows~n", []),
    {{Rows,_,_}, NewBuf} = get_rows_from(Buf, 1, 5),
    io:format(user, "rows pre-modify ~p~n", [Rows]),
    modify_rows(NewBuf, upd, [   ["3",31,33,34]
                              ,  ["5",51,53,54]
    ]),
    io:format(user, "updated 3,5~n", []),
    modify_rows(NewBuf, del, [   ["2",21,23,24]
    ]),
    io:format(user, "deleted 2~n", []),
    modify_rows(NewBuf, ins, [   [71,73,74]
                              ,  [81,83,84]
    ]),
    io:format(user, "added 2~n", []),
    {{NewRows2,_,_}, _} = get_rows_from(NewBuf, 1, 20),
    io:format(user, "rows post-modify ~p~n", [NewRows2]),
    NewRows3 = get_modified_rows(NewBuf),
    io:format(user, "modifed ~p~n", [NewRows3]),
    delete(NewBuf),
    io:format(user, "-------------------~n", []).

insert_tuples(0, _) -> ok;
insert_tuples(N, Buf) ->
    true = insert_rows(Buf, [[{table, [64+N],[65+N],[66+N]}]]),
    insert_tuples(N-1, Buf).

read_all_fwd(Buf, Chunk, Acc) ->
    {{Rows,_,_}, NewBuf} = get_next_rows(Buf, Chunk),
    case Rows of
        [] -> {Acc, Buf};
        Rows -> read_all_fwd(NewBuf, Chunk, Acc ++ Rows)
    end.

read_all_bk(Buf, Chunk, Acc) ->
    {{Rows,_,_}, NewBuf} = get_prev_rows(Buf, Chunk),
    case Rows of
        [] -> {Acc, Buf};
        Rows -> read_all_bk(NewBuf, Chunk, Acc ++ Rows)
    end.

% - read_test(_) ->
% -     io:format(user, "---- read test ----~n", []),
% -     Buf = create(fun rfun/1),
% -     RowCount = 9,
% -     ok = insert_many(RowCount, Buf),
% -     io:format(user, "inserted ~p rows~n", [RowCount]),
% -     {Rows, NewBuf} = read_all_fwd(Buf, 3, []),
% -     io:format(user, "read forward ~p~n", [Rows]),
% -     {NewRows, NewBuf1} = read_all_bk(NewBuf, 3, []),
% -     io:format(user, "read backward ~p~n", [NewRows]),
% -     {{NewRows1,_,_}, NewBuf2} = get_rows_from(NewBuf1, 3, 9),
% -     io:format(user, "read middle-end ~p~n", [NewRows1]),
% -     {{NewRows2,_,_}, _} = get_rows_from(NewBuf2, 1, 4),
% -     io:format(user, "read middle-start ~p~n", [NewRows2]),
% -     delete(Buf),
% -     io:format(user, "-------------------~n", []).

% - insert_many(0, _) -> ok;
% - insert_many(N, Buf) ->
% -     true = insert_rows(Buf, [[[64+N],[65+N],[66+N]]]),
% -     insert_many(N-1, Buf).
