-module(nirvana).


-export([init/0,
         add_user/2,
         add_user/3,
         add_route/2,
         add_username/2,
         get_route_by_uid/1,
         get_username/1,
         get_route_by_username/1,
         call_by_uid/4,
         cast_by_uid/4,
         hungries/0,
         rand/1]).

-define(MaxUsers,1000).
-define(Tables, [nirvana_routes, nirvana_usernames]).

-record(nirvana_routes, {uid :: binary(), node :: node()}).
-record(nirvana_usernames, {username :: binary(), uid :: binary()}).

-type route() :: #nirvana_routes{}.
-type username() ::#nirvana_usernames{}.


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
         {type, set}]).

%%
%% Users interface
%%

-spec add_user(binary(), binary()) -> {aborted, _} | {atomic, ok}.
add_user(Uid, Username) ->
    add_user(Uid, Username, node()).

-spec add_user(binary(), binary(), node()) -> {aborted, _} | {atomic, ok}.
add_user(Uid, Username, Node) ->
    mnesia:transaction(fun() ->
                case {add_route(Uid, Node), add_username(Username, Uid)} of
                    {{atomic, ok}, {atomic, ok}} ->
                        ok;
                    Else ->
                        Else
                end
        end).

-spec add_route(binary(), node()) -> {aborted, _} | {atomic, ok}.
add_route(Uid, Node) ->
    mnesia:transaction(fun() ->
                mnesia:write(#nirvana_routes{uid=Uid, node=Node})
        end).

-spec add_username(binary(), binary()) -> {aborted, _} | {atomic, ok}.
add_username(Username, Uid) ->
    mnesia:transaction(fun() ->
                mnesia:write(#nirvana_usernames{username=Username, uid=Uid})
        end).

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
                    [#nirvana_usernames{uid=Uid}] ->
                        {Username, Uid};
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
    case lists:filter(fun hungry/1, Nodes) of
        [] ->
            [];
        Else ->
            Else
    end.

-spec hungry(node()) -> true | false.
hungry(Node) ->
    case count_users_on(Node) of
        {atomic, Count} ->
            Count < ?MaxUsers;
        {aborted, _} ->
            false
    end.


%%
%% Some help
%%

-spec rand([A]) -> A.
rand(List) ->
    lists:nth(random:uniform(length(List)), List).
