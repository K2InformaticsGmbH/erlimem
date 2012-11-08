-module(buffer_mgr).

-record(buffer,
    { row_top = 0
    , row_bottom = 0
    , tableid
    }).

-export([create_buffer/0
        , get_rows_from_ets/3
        , insert_rows/2
        , get_prev_rows/3
        , get_rows_from/4
        , get_next_rows/3
        ]).

create_buffer() ->
    #buffer{tableid=ets:new(results, [ordered_set, public])}.

get_rows_from_ets(#buffer{row_top=RowStart, row_bottom=RowEnd} = Buffer, TableId, Columns) ->
    Keys = lists:seq(RowStart, RowEnd),
    Rows = [ets:lookup(TableId, K1)||K1<-Keys],
    {[[integer_to_list(I)|format_row(Columns,R)]
     || [{I,R}|_] <-Rows], Buffer}.

format_row(Cols,Rows) -> format_row(Cols,Rows,[]).
format_row([],[], Acc) -> Acc;
format_row([{_,date,_}|Columns],[R|Rows], Acc) ->
    <<Y:32, Mon:16, D:16, H:16, M:16, S:16>> = list_to_binary(R),
    Date = binary_to_list(list_to_binary([<<D:16>>, ".", <<Mon:16>>, ".", <<Y:32>>, " ", <<H:16>>, ":", <<M:16>>, ":", <<S:16>>])),
    format_row(Columns, Rows, Acc ++ [Date]);
format_row([_|Columns],[R|Rows], Acc) -> format_row(Columns, Rows, Acc ++ [R]).


insert_rows(#buffer{tableid=Results}, Rows) ->
    NrOfRows = length(Rows),
    CacheSize = ets:info(Results, size),
    ets:insert(Results, [{I, R}||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), lists:reverse(Rows))]).

get_prev_rows(#buffer{row_top=RowTop, tableid=TableId} = Buffer, MaxRows, Columns) ->
    CacheSize = ets:info(TableId, size),
    NewRowTop =
        if (RowTop - MaxRows - 1) < 1 -> 1;
           true -> (RowTop - MaxRows - 1)
        end,
    NewRowBottom =
        if (NewRowTop + MaxRows) > CacheSize -> CacheSize;
           true -> (NewRowTop + MaxRows)
        end,
    get_rows_from_ets(Buffer#buffer{row_top=NewRowTop,row_bottom=NewRowBottom}, TableId, Columns).

get_rows_from(#buffer{tableid=TableId} = Buffer, RowNum, MaxRows, Columns) ->
    CacheSize = ets:info(TableId, size),
    NewRowBottom =
        if (RowNum > CacheSize) or (RowNum + MaxRows > CacheSize) -> CacheSize;
           true -> RowNum + MaxRows
        end,
    NewRowTop =
        if (NewRowBottom - MaxRows) < 1 -> 1;
           true -> (NewRowBottom - MaxRows)
        end,
    get_rows_from_ets(Buffer#buffer{row_top=NewRowTop,row_bottom=NewRowBottom}, TableId, Columns).

get_next_rows(#buffer{row_bottom=RowBottom, tableid=TableId} = Buffer, MaxRows, Columns) ->
    CacheSize = ets:info(TableId, size),
    NewRowBottom =
        if (RowBottom + 1 + MaxRows) > CacheSize -> CacheSize;
            true -> (RowBottom + 1 + MaxRows)
        end,
    NewRowTop =
        if (NewRowBottom - MaxRows) < 1 -> 1;
            true -> (NewRowBottom - MaxRows)
        end,
    get_rows_from_ets(Buffer#buffer{row_top=NewRowTop,row_bottom=NewRowBottom}, TableId, Columns).
