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
   name      = undefined :: atom()           % unique name-space id
  ,factory   = undefined :: pid()            % process factory or undefined
  ,keylen    = inf       :: integer() | inf  % length of key (key prefix used to distinguish a process)
  ,readonly  = false     :: boolean()        % write operations are disabled
  ,rthrough  = false     :: boolean()        % read-through
  ,immutable = false     :: boolean()        % write-once (written value cannot be changed)
  ,protocol  = pipe      :: pipe | otp       % communication protocol 
  ,entity                :: any()            % entity specification (for accounting only)
  ,capacity  = inf       :: integer()        % max number of elements
}).
