-module(nirvana).


-export([init/0,
         add_user/0,
         add_user/1,
         add_user/2,
         add_user/3,
         add_route/2,
         add_token/1,
         add_username/2,
         get_token/1,
         get_route_by_uid/1,
         get_username/1,
         get_route_by_username/1,
         call_by_uid/4,
         cast_by_uid/4,
         hungries/0,
         rand/1]).

-define(MaxUsers,1000).
-define(Tables, [nirvana_routes, nirvana_usernames, nirvana_tokens]).

-record(nirvana_routes, {uid :: binary(), node :: node()}).
-record(nirvana_usernames, {username :: binary(), password :: binary() | none, uid :: binary()}).
-record(nirvana_tokens, {token :: binary(), uid :: binary()}).

-type route() :: #nirvana_routes{}.
-type username() ::#nirvana_usernames{}.
-type token() :: #nirvana_tokens{}.


%% @doc a helper getting a node up and going
-spec init() -> {ok, init | load | join_init | join_load}.
init() ->
    case app_on(mnesia) of
        none ->
            case {mnesia:create_schema([node()]), application:start(mnesia)} of
                {ok, ok} ->
                    lists:foreach(fun(T) -> {atomic, ok} = create_table(T) end, ?Tables),
                    {ok, init};
                {{error, {_, {already_exists, _}}}, ok} ->
                    {ok, load}
            end;
        Node ->
            ok = application:start(mnesia),
            {ok, _} = mnesia:change_config(extra_db_nodes, [Node]),
            case mnesia:change_table_copy_type(schema, node(), disc_copies) of
                {atomic, ok} ->
                    lists:foreach(fun(T) ->
                                {atomic, ok} = mnesia:add_table_copy(T, node(), disc_copies)
                        end, ?Tables),
                    {ok, join_init};
                {aborted, {already_exists, _, _, _}} ->
                    {ok, join_load}
            end
    end.

-spec app_on(atom()) -> node().
app_on(App) ->
    app_on(App, nodes()).

-spec app_on(atom(), []) -> none;
            (atom(), [node()]) -> node() | none.
app_on(_App, []) ->
    none;
app_on(App, [Node|Nodes]) ->
    case rpc:call(Node, application, get_application, [mnesia]) of
        {ok, mnesia} ->
            Node;
        undefined ->
            app_on(App, Nodes)
    end.


create_table(Table) ->
    create_table(Table, [node()]).

create_table(nirvana_routes, Nodes) ->
    mnesia:create_table(nirvana_routes,
        [{attributes, record_info(fields, nirvana_routes)},
         {index, [#nirvana_routes.node]},
         {disc_copies, Nodes},
         {type, set}]);
create_table(nirvana_usernames, Nodes) ->
    mnesia:create_table(nirvana_usernames,
        [{attributes, record_info(fields, nirvana_usernames)},
         {disc_copies, Nodes},
         {type, set}]);
create_table(nirvana_tokens, Nodes) ->
    mnesia:create_table(nirvana_tokens,
        [{attributes, record_info(fields, nirvana_tokens)},
         {disc_copies, Nodes},
         {type, set}]).

%%
%% Users interface
%%

%% @doc create a user on this node, automatically generating
%% the uid and username
add_user() ->
    Uid = uuid:get_v4(),
    Friendly = list_to_binary(uuid:uuid_to_string(Uid)),
    add_user(Uid, <<"sherpa:", Friendly/binary>>).

add_user(Username) ->
    add_user(Username, uuid:get_v4()).

add_user(Username, Uid) ->
    add_user(Uid, Username, none).

add_user(Username, Uid, Password) ->
    add_user(Username, Uid, Password, node()).

add_user(Username, Uid, Password, Node) ->
    Fun = fun() ->
            case {add_route(Uid, Node), add_username(Username, Password, Uid), add_token(Uid)} of
                {{ok, _}, {ok, _}, {ok, {Token, _}}} ->
                    {ok, [{uid, Uid},
                            {username, Username},
                            {token, Token},
                            {node, Node}]};
                Else ->
                    {error, Else}
            end
    end,
    add_user(Username, Uid, Password, Node, mnesia:transaction(Fun)).

add_user(_Username, _Uid, _Password, _Node, {aborted, Reason})       -> {error, Reason};
add_user(_Username, _Uid, _Password, _Node, {atomic, {ok, Result}})  -> {ok, Result}.


-spec add_route(binary(), node()) -> {aborted, _} | {atomic, ok}.
add_route(Uid, Node) ->
    Fun = fun() ->
            mnesia:write(#nirvana_routes{uid=Uid, node=Node})
    end,
    case mnesia:transaction(Fun) of
        {aborted, Reason} ->
            {error, Reason};
        {atomic, ok} ->
            {ok, {Uid, Node}}
    end.

-spec add_username(binary(), binary(), binary()) -> {aborted, _} | {atomic, ok}.
add_username(Username, Password, Uid) ->
    Fun = fun() ->
            mnesia:write(#nirvana_usernames{username=Username,
                                            uid=Uid,
                                            password=Password})
    end,
    case mnesia:transaction(Fun) of
        {aborted, Reason} ->
            {error, Reason};
        {atomic, ok} ->
            {ok, {Username, Uid}}
    end.

-spec add_token(U) -> {error, _} | {ok, {binary(), U}}.
add_token(Uid) ->
    add_token(token(Uid), Uid).

-spec add_token(T, U) -> {error, _} | {ok, {T, U}}.
add_token(Token, Uid) ->
    Fun = fun() ->
            mnesia:write(#nirvana_tokens{token=Token, uid=Uid})
    end,
    case mnesia:transaction(Fun) of
        {aborted, Reason} ->
            {error, Reason};
        {atomic, ok} ->
            {ok, {Token, Uid}}
    end.

%% Help for user properties

token(Uid) ->
    token(Uid, calendar:now_to_datetime(erlang:now())).

token(Uid, Datetime) ->
    Timestamp = timestamp(Datetime),
    Readable = list_to_binary(uuid:uuid_to_string(Uid)),
    Rand = list_to_binary(io_lib:format("~..0B", [random:uniform(1 bsl 64) + (1 bsl 32)])),
    <<Timestamp/binary, ".", Readable/binary, ".", Rand/binary>>.

timestamp({{Y, M, D}, {H, Mi, S}}) ->
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0B.~2..0B~2..0B~2..0B", [Y, M, D, H, Mi, S])).


%% Gets

-spec get_route_by_uid(binary()) -> {aborted, _} | {atomic, route() | undefined}.
get_route_by_uid(Uid) ->
    mnesia:transaction(fun() ->
                case mnesia:read({nirvana_routes, Uid}) of
                    [#nirvana_routes{node=Node}] ->
                        {Uid, Node};
                    [] ->
                        undefined
                end
        end).


-spec get_username(binary()) -> {aborted, _} | {atomic, username() | undefined}.
get_username(Username) ->
    mnesia:transaction(fun() ->
                case mnesia:read({nirvana_usernames, Username}) of
                    [#nirvana_usernames{password=Password, uid=Uid}] ->
                        {Username, Password, Uid};
                    [] ->
                        undefined
                end
        end).

-spec get_token(binary()) -> {aborted, _} | {atomic, token() | undefined}.
get_token(Token) ->
    mnesia:transaction(fun() ->
                case mnesia:read({nirvana_tokens, Token}) of
                    [#nirvana_tokens{uid=Uid}] ->
                        {Token, Uid};
                    [] ->
                        undefined
                end
        end).

-spec get_route_by_username(binary()) -> {aborted, _} | {atomic, route() | undefined}.
get_route_by_username(Username) ->
    mnesia:transaction(fun() ->
                case get_username(Username) of
                    {atomic, undefined} ->
                        undefined;
                    {atomic, {_, Uid}} ->
                        case get_route_by_uid(Uid) of
                            {atomic, undefined} ->
                                undefined;
                            {atomic, Route} ->
                                Route
                        end
                end
        end).

-spec count_users_on(node()) -> integer().
count_users_on(Node) ->
    mnesia:transaction(fun() ->
                mnesia:foldr(fun(#nirvana_routes{node=RowNode}, Acc) ->
                           if RowNode == Node -> Acc + 1;
                               true -> Acc
                           end
                    end, 0, nirvana_routes)
        end).

%%
%% Routing wrappers
%%

-spec call_by_uid(binary(), atom(), atom(), [_]) -> _ | {undefined, uid}.
call_by_uid(Uid, Mod, Func, Args) ->
    case get_route_by_uid(Uid) of
        {atomic, {_Uid, Node}} ->
            rpc:call(Node, Mod, Func, Args);
        {atomic, undefined} ->
            {undefined, uid};
        Else ->
            Else
    end.

-spec cast_by_uid(binary(), atom(), atom(), [_]) -> true | {undefined, uid} | _.
cast_by_uid(Uid, Mod, Func, Args) ->
    case get_route_by_uid(Uid) of
        {atomic, {_Uid, Node}} ->
            rpc:cast(Node, Mod, Func, Args);
        {atomic, undefined} ->
            {undefined, uid};
        Else ->
            Else
    end.

%% @doc A hungry node is one which has more room for users
-spec hungries() -> node().
hungries() ->
    hungries([node()|nodes()]).

-spec hungries([node()]) -> node().
hungries(Nodes) ->
    lists:filter(fun hungry/1, Nodes).

-spec hungry(node()) -> true | false.
hungry(Node) ->
    hungry(Node, count_users_on(Node)).

hungry(_Node, {atomic, Count}) -> Count < ?MaxUsers;
hungry(_Node, {aborted, _})    -> false.


%%
%% Some help
%%

-spec rand([A]) -> A.
rand(List) ->
    lists:nth(random:uniform(length(List)), List).
