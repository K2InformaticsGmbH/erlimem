-record(gres,   { %% response sent back .. gui
                  operation           %% rep (replace) | app (append) | prp (prepend) | nop | close
                , cnt = 0             %% current buffer size (raw table or index table size)
                , toolTip = <<"">>    %% current buffer sizes RawCnt/IndCnt plus status information
                , message = <<"">>    %% error message
                , beep = false        %% alert with a beep if true
                , state = empty       %% determines color of buffer size indicator
                , loop = <<"">>       %% gui should come back with this command -- empty string is 'undefined'
                , rows = []           %% rows .. show (append / prepend / merge)
                , keep = 0            %% row count .. be kept
                }).
