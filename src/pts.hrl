%-define(CONFIG_DEBUG, true).

%% default timeout
-define(DEF_TIMEOUT,  5000).

-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), error_logger:info_msg(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.

