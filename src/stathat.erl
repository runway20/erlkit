%%
%% @author Patrick Crosby <patrick@stathat.com>
%% @author Sam Elliott <sam@lenary.co.uk>
%% @version 0.2
%% @doc Gen Server for sending data to stathat.com stat tracking service.
%%
%% <h4>Example:</h4>
%% <pre><code>
%% 1&gt; {ok, Pid} = stathat:start().
%% 2&gt; stathat:ez_count("erlang@stathat.com", "messages sent", 1).
%% ok.
%% 3&gt; stathat:ez_value("erlang@stathat.com", "request time", 92.194).
%% ok.
%%

-module(stathat).

-author("Patrick Crosby <patrick@stathat.com>").
-author("Sam Elliott <sam@lenary.co.uk").
-version("0.2").

-behaviour(gen_server).

-export([
         start/0,
         start_link/0, start_link/1, start_link/2,
         count/1, count/2, count/3,
         value/2, value/3
        ]).

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).


-define(SH_BASE_URL(X), "http://api.stathat.com/" ++ X).

-record(state, {ezkey = undefined :: string(),
                namespace = undefined :: string()}).


%% Public API

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    % Attempt to get the default ezkey from the stathat:ezkey env var
    start_link(application:get_env(?MODULE, ezkey)).

start_link(Ezkey) ->
    start_link(Ezkey, application:get_env(?MODULE, namespace)).

start_link(Ezkey, Namespace) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Ezkey, Namespace}, []).

%% @doc Post a count using the ezkey
count(Stat) ->
    count(Stat, 1).

count(Stat, Count) ->
    gen_server:cast(?MODULE, {count, Stat, Count}).

count(Ezkey, Stat, Count) ->
    gen_server:cast(?MODULE, {count, Ezkey, Stat, Count}).

%% @doc Post a value using the ezkey
value(Stat, Value) ->
    gen_server:cast(?MODULE, {value, Stat, Value}).

value(Ezkey, Stat, Value) ->
    gen_server:cast(?MODULE, {value, Ezkey, Stat, Value}).


% gen_server callbacks

init({Ezkey, Namespace}) ->
    State = #state{ezkey=Ezkey, namespace=Namespace},
    case inets:start() of
        ok ->
            {ok, State};
        {error, {already_started, inets}} ->
            {ok, State};
        {error, Err} ->
            {stop, Err}
    end.


%% @doc no calls supported
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.


handle_cast({ez_count, Stat, Count}, #state{ezkey=Ezkey} = State) when Ezkey =/= undefined ->
    handle_cast({ez_count, Ezkey, Stat, Count}, State);
handle_cast({ez_count, Ezkey, Stat, Count}, #state{namespace=Namespace} = State) ->
    Url = build_url("ez", [{"ezkey", Ezkey}, {"stat", ns_stat(Namespace, Stat)}, {"count", ntoa(Count)}]),
    httpc:request(get, {?SH_BASE_URL(Url), []}, [], [{sync, false}]),
    {noreply, State};

handle_cast({ez_value, Stat, Value}, #state{ezkey=Ezkey} = State) when Ezkey =/= undefined ->
    handle_cast({ez_value, Ezkey, Stat, Value}, State);
handle_cast({ez_value, Ezkey, Stat, Value}, #state{namespace=Namespace} = State) ->
    Url = build_url("ez", [{"ezkey", Ezkey}, {"stat", ns_stat(Namespace, Stat)}, {"value", ntoa(Value)}]),
    httpc:request(get, {?SH_BASE_URL(Url), []}, [], [{sync, false}]),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({_RequestId, {error, _Reason}}, State) ->
    % You could do something here, but I won't
    {noreply, State};
handle_info({_RequestId, _Result}, State) ->
    % Again, you might do something here but I won't
    {noreply, State};
handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private methods

ntoa(Num) when is_list(Num) ->
    Num;
ntoa(Num) when is_float(Num) ->
    lists:flatten(io_lib:format("~f", [Num]));
ntoa(Num) when is_integer(Num) ->
    integer_to_list(Num).

%% url utility functions (borrowed from twitter_client module)

build_url(Url, []) -> Url;
build_url(Url, Args) ->
    Url ++ "?" ++ lists:concat(
                    lists:foldl(
                      fun (Rec, []) -> [Rec]; (Rec, Ac) -> [Rec, "&" | Ac] end, [],
                      [K ++ "=" ++ http_uri:encode(V) || {K, V} <- Args]
                     )
                   ).


%% @doc namespace a statistic
ns_stat(undefined, Stat) ->
    Stat;
ns_stat(Namespace, Stat) ->
    Namespace ++ "_" ++ Stat.
