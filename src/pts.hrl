%-define(CONFIG_DEBUG, true).

%%
%% define registry
-define(CONFIG_REGISTRY, [
   public
  ,named_table
  ,set
  ,{read_concurrency, true}
  ,{keypos, 2}
]).


%% default timeout
-define(DEF_TIMEOUT,  5000).

-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), error_logger:info_msg(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.


%% process buckets record
-record(pts, {
   name              :: atom(),          % unique name-space id
   factory           :: pid(),           % process factory or undefined
   global    = undefined :: atom(),      % process group is global
   keylen    = inf   :: integer() | inf, % length of key (key prefix used to distinguish a process)
   readonly  = false :: boolean(),       % write operations are disabled
   rthrough  = false :: boolean(),       % read-through
   immutable = false :: boolean(),       % write-once (written value cannot be changed)
   entity            :: any()            % entity specification (for accounting only)
}).
