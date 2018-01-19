-module(tanodb_call_fsm).
-behavior(gen_fsm).

%% API
-export([start_link/4, call/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

-ignore_xref([start_link/7, init/1, code_change/4, handle_event/3,
              handle_info/3, handle_sync_event/4, terminate/3, write/6,
              delete/4]).

-export([prepare/2, execute/2, execute2/2, execute3/2]).

-ignore_xref([prepare/2, execute/2, waiting/2]).

%% req_id: The request id so the caller can verify the response.
%% sender: The pid of the sender so a reply can be made.
%% prelist: The preflist for the given {Bucket, Key} pair.
%% num_w: The number of successful write replies.
-record(state, {src :: string(),
                dst :: string(),
                payload :: any(),
                req_id :: pos_integer(),
                preflist :: riak_core_apl:preflist2()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(SRC, DST, Payload, ReqID) ->
    gen_fsm:start_link(?MODULE, [SRC, DST, Payload, ReqID],
                       []).

call(SRC, DST, Payload, ReqID) ->
    io:format(user, "Call ~p~n", [DST]),
    tanodb_call_fsm_sup:start_call_fsm([SRC, DST, Payload, ReqID]),
    {ok, ReqID}.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([SRC, DST, Payload, ReqID]) ->
    SD = #state{req_id=ReqID, src=SRC, dst=DST, payload=Payload},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{dst=DST}) ->
    N = 3,
    DocIdx = riak_core_util:chash_key(DST),
    Preflist = riak_core_apl:get_apl(DocIdx, N, tanodb),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{src=SRC, dst=DST, payload=Payload, req_id = ReqID,
                            preflist=[Node|RestPreflist]}) ->
    Command = {call, SRC, DST, Payload, ReqID},
    riak_core_vnode_master:command(Node, Command, {fsm, undefined, self()},
                                   tanodb_vnode_master),
    SD=SD0#state{preflist=RestPreflist},
    {next_state, execute2, SD, 5000}.

execute2(timeout, SD0=#state{src=SRC, dst=DST, payload=Payload, req_id = ReqID,
                            preflist=[Node|RestPreflist]}) ->
    Command = {call, SRC, DST, Payload, ReqID},
    riak_core_vnode_master:command(Node, Command, {fsm, undefined, self()},
                                   tanodb_vnode_master),
    SD=SD0#state{preflist=RestPreflist},
    {next_state, execute3, SD, 5000};
execute2({try_again, ReqID}, SD0=#state{req_id=ReqID}) ->
  execute2(timeout, SD0);
execute2(_, SD0) ->
  {stop, normal, SD0}.

execute3(timeout, SD0=#state{src=SRC, dst=DST, payload=Payload, req_id = ReqID,
                            preflist=[Node|RestPreflist]}) ->
    Command = {call, SRC, DST, Payload, ReqID},
    riak_core_vnode_master:command(Node, Command, {fsm, undefined, self()},
                                   tanodb_vnode_master),
    SD=SD0#state{preflist=RestPreflist},
    {stop, badmsg, SD};
execute3({try_again, ReqID}, SD0=#state{req_id=ReqID}) ->
  execute3(timeout, SD0);
execute3(_, SD0) ->
  {stop, ok, SD0}.



handle_info(Info, _StateName, StateData) ->
    lager:warning("got unexpected info ~p", [Info]),
    {stop, badmsg, StateData}.

handle_event(Event, _StateName, StateData) ->
    lager:warning("got unexpected event ~p", [Event]),
    {stop, badmsg, StateData}.

handle_sync_event(Event, _From, _StateName, StateData) ->
    lager:warning("got unexpected sync event ~p", [Event]),
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
terminate(_Reason, _SN, _SD) -> ok.
