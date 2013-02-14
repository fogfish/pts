
%%
%% internal record, pts meta-data
-record(pts, {
   ns        :: atom(),                 % unique name-space id
   keylen    = inf   :: integer() | inf, % length of key (significant park of key used to distinguish a process)
   readonly  = false :: boolean(),      % write operations are disabled
   rthrough  = false :: boolean(),      % read-through
   immutable = false :: boolean(),      % write-once (written value cannot be changed)
   supervisor        :: atom() | pid(), % element supervisor (simple_one_for_one)
   attempt   = 5     :: integer(),      % number of attempts to acquire lock
   owner                                % owner process pid
}).
-define(TIMEOUT, 10000).