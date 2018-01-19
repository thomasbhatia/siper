%% @doc Supervise the tanodb_write FSM.
-module(tanodb_call_fsm_sup).
-behavior(supervisor).

-export([start_call_fsm/1, start_link/0]).
-export([init/1]).

-ignore_xref([init/1, start_link/0]).

start_call_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    WriteFsm = {undefined,
                {tanodb_call_fsm, start_link, []},
                temporary, 5000, worker, [tanodb_call_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [WriteFsm]}}.
