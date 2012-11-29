-module(erlimem_buf).

-record(buffer,
    { row_top = 0
    , row_bottom = 0
    , tableid
    , rowfun
    }).

-export([create_buffer/1
        , delete_buffer/1
        , get_rows_from_ets/3
        , insert_rows/2
        , get_prev_rows/3
        , get_rows_from/4
        , get_next_rows/3
        , get_buffer_max/1
        , rfun/1
        ]).

create_buffer(Fun) ->
    #buffer{tableid=ets:new(results, [ordered_set, public])
           , rowfun = Fun
    }.

delete_buffer(#buffer{tableid=Tab}) ->
    true = ets:delete(Tab).

% - format_row(Cols, Row) when is_tuple(Row) -> format_row(Cols,lists:nthtail(1, tuple_to_list(Row)), []);
% - format_row(Cols, Row)                    -> format_row(Cols,Row,[]).
% - format_row([],[], Acc) -> Acc;
% - format_row([{_,date,_}|Columns],[R|Rows], Acc) ->
% -     <<Y:32, Mon:16, D:16, H:16, M:16, S:16>> = list_to_binary(R),
% -     Date = binary_to_list(list_to_binary([<<D:16>>, ".", <<Mon:16>>, ".", <<Y:32>>, " ", <<H:16>>, ":", <<M:16>>, ":", <<S:16>>])),
% -     format_row(Columns, Rows, Acc ++ [Date]);
% - format_row([_|Columns],[R|Row], Acc) -> format_row(Columns, Row, Acc ++ [R]);
% - format_row([],[R|Row], Acc) -> format_row([], Row, Acc ++ [R]).
% - 

insert_rows(#buffer{tableid=TableId}, Rows) ->
    NrOfRows = length(Rows),
    CacheSize = ets:info(TableId, size),
    ets:insert(TableId, [list_to_tuple([I|R])||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), lists:reverse(Rows))]).

% TODO - complete state of the buffer (true/false) need to be determined
get_rows_from_ets(#buffer{row_top=RowStart, row_bottom=RowEnd, rowfun=F}, TableId, _Columns) ->
    CacheSize = ets:info(TableId, size),
    [FirstRow] = ets:lookup(TableId, 1),
    {MatchHead, MatchExpr} = build_match(size(FirstRow)),
    Rows = ets:select(TableId,[{MatchHead,[{'>=','$1',RowStart},{'=<','$1',RowEnd}],[MatchExpr]}]),
    NewRows = [F(R) || R <- Rows],
    {NewRows, true, CacheSize}.

rfun([I|R]) -> [integer_to_list(I)|R].
    
build_match(Count) -> build_match(Count, {[],[]}).
build_match(0, {Head,Expr}) -> {list_to_tuple(Head),Expr};
build_match(2, {Head,Expr}) -> build_match(1, {['_'|Head],Expr});
build_match(Count, {Head,Expr}) ->
    MVar = list_to_atom("$" ++ integer_to_list(Count)),
    build_match(Count-1, {[MVar|Head], [MVar|Expr]}).
    
% TODO - complete state of the buffer (true/false) need to be determined
get_buffer_max(#buffer{tableid=TableId}) ->
    {ok, true, ets:info(TableId, size)}.

get_rows_from(#buffer{tableid=TableId} = Buf, RowNum, MaxRows, Columns) ->
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
    {get_rows_from_ets(NewBuf, TableId, Columns), NewBuf}.

get_prev_rows(#buffer{row_top=RowTop, tableid=TableId} = Buf, MaxRows, Columns) ->
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
            {[], NewBuf};
        true ->
            {Rows,_,_} = Result = get_rows_from_ets(NewBuf, TableId, Columns),
            io:format(user, "Rows ~p OldBuf ~p NewBuf ~p~n", [length(Rows), Buf, NewBuf]),
            {Result, NewBuf}
    end.

get_next_rows(#buffer{row_bottom=RowBottom, tableid=TableId} = Buf, MaxRows, Columns) ->
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
            {[], NewBuf};
        true ->
            {Rows,_,_} = Result = get_rows_from_ets(NewBuf, TableId, Columns),
            io:format(user, "Rows ~p OldBuf ~p NewBuf ~p~n", [length(Rows), Buf, NewBuf]),
            {Result, NewBuf}
    end.

% EUnit tests --

-include_lib("eunit/include/eunit.hrl").

setup() -> create_buffer(fun rfun/1).
teardown(Buf) -> delete_buffer(Buf).

db_test_() ->
    {timeout, 100000, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun read_all_test/1
        ]}
        }
    }.

read_all_test(Buf) ->
    ok = insert_many(9, Buf),
    {Rows, NewBuf} = read_all_fwd(Buf, 3, []),
    io:format(user, "read forward ~p rows~n", [length(Rows)]),
    {NewRows, NewBuf1} = read_all_bk(NewBuf, 3, []),
    io:format(user, "read backward ~p rows~n", [length(NewRows)]),
    {NewRows1, NewBuf2} = get_rows_from(NewBuf1, 3, 9, [{},{},{}]),
    io:format(user, "read middle-end ~p rows~n", [length(NewRows1)]),
    {NewRows2, _} = get_rows_from(NewBuf2, 1, 4, [{},{},{}]),
    io:format(user, "read middle-start ~p rows~n", [length(NewRows2)]),
    ok.

insert_many(0, _) -> ok;
insert_many(N, Buf) ->
    true = insert_rows(Buf, [[N,N,N]]),
    insert_many(N-1, Buf).

read_all_fwd(Buf, Chunk, Acc) ->
    {Rows, NewBuf} = get_next_rows(Buf, Chunk, [{},{},{}]),
    case Rows of
        [] -> {Acc, Buf};
        Rows -> read_all_fwd(NewBuf, Chunk, Acc ++ Rows)
    end.

read_all_bk(Buf, Chunk, Acc) ->
    {Rows, NewBuf} = get_prev_rows(Buf, Chunk, [{},{},{}]),
    case Rows of
        [] -> {Acc, Buf};
        Rows -> read_all_bk(NewBuf, Chunk, Acc ++ Rows)
    end.
