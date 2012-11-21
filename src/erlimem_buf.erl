-module(erlimem_buf).

-record(buffer,
    { row_top = 0
    , row_bottom = 0
    , tableid
    }).

-export([create_buffer/0
        , delete_buffer/1
        , get_rows_from_ets/3
        , insert_rows/2
        , get_prev_rows/3
        , get_rows_from/4
        , get_next_rows/3
        ]).

create_buffer() ->
    #buffer{tableid=ets:new(results, [ordered_set, public])}.

delete_buffer(#buffer{tableid=Tab}) ->
    true = ets:delete(Tab).

format_row(Cols, Row) when is_tuple(Row) -> format_row(Cols,lists:nthtail(1, tuple_to_list(Row)), []);
format_row(Cols, Row)                    -> format_row(Cols,Row,[]).
format_row([],[], Acc) -> Acc;
format_row([{_,date,_}|Columns],[R|Rows], Acc) ->
    <<Y:32, Mon:16, D:16, H:16, M:16, S:16>> = list_to_binary(R),
    Date = binary_to_list(list_to_binary([<<D:16>>, ".", <<Mon:16>>, ".", <<Y:32>>, " ", <<H:16>>, ":", <<M:16>>, ":", <<S:16>>])),
    format_row(Columns, Rows, Acc ++ [Date]);
format_row([_|Columns],[R|Row], Acc) -> format_row(Columns, Row, Acc ++ [R]);
format_row([],[R|Row], Acc) -> format_row([], Row, Acc ++ [R]).


insert_rows(#buffer{tableid=Results}, Rows) ->
    NrOfRows = length(Rows),
    CacheSize = ets:info(Results, size),
    ets:insert(Results, [{I, R}||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), lists:reverse(Rows))]).

get_rows_from_ets(#buffer{row_top=RowStart, row_bottom=RowEnd}, TableId, Columns) ->
    Keys = lists:seq(RowStart, RowEnd),
    Rows = [ets:lookup(TableId, K1)||K1<-Keys],
    [[integer_to_list(I)|format_row(Columns,R)] || [{I,R}|_] <-Rows].

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
            Rows = get_rows_from_ets(NewBuf, TableId, Columns),
            io:format(user, "Rows ~p OldBuf ~p NewBuf ~p~n", [length(Rows), Buf, NewBuf]),
            {Rows, NewBuf}
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
            Rows = get_rows_from_ets(NewBuf, TableId, Columns),
            io:format(user, "Rows ~p OldBuf ~p NewBuf ~p~n", [length(Rows), Buf, NewBuf]),
            {Rows, NewBuf}
    end.

% EUnit tests --

-include_lib("eunit/include/eunit.hrl").

setup() -> create_buffer().
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
