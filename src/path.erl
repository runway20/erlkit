-module(path).

-export([foldl/3,
         foldl/4,
         foldr/3,
         foldr/4,
         head/1,
         head/2,
         head/3,
         last/1,
         last/2,
         last/3]).

foldl(Tree, Fun, Acc) ->
    foldl(Tree, Fun, Acc, {undefined, undefined}).

foldl(Tree, Fun, Acc, {_, _} = Bounds) ->
    foldl(Tree, Fun, Acc, Bounds, fun lists:usort/1);
foldl(Tree, Fun, Acc, Order) when is_function(Order) ->
    foldl(Tree, Fun, Acc, {undefined, undefined}, Order).

foldl(Path, Fun, Acc, Bounds, Order) when is_binary(Path) ->
    foldl([binary_to_list(Path)], Fun, Acc, Bounds, Order);
foldl([C|_] = Path, Fun, Acc, Bounds, Order) when is_integer(C) ->
    foldl([Path], Fun, Acc, Bounds, Order);
foldl([], _, Acc, _, _) ->
    Acc;
foldl([Path|_], _, Acc, {_, Upper}, _) when Upper =/= undefined, Path >= Upper ->
    Acc;
foldl([Path|Tail], Fun, Acc, {Lower, Upper}, Order) when Lower =:= undefined; Path >= Lower ->
    case file:list_dir(Path) of
        {ok, Paths} ->
            foldl([filename:join(Path, F) || F <- Order(Paths)] ++ Tail, Fun, Acc, {Lower, Upper}, Order);
        {error, enotdir} ->
            foldl(Tail, Fun, Fun(Path, Acc), {Lower, Upper}, Order);
        {error, enoent} ->
            foldl(Tail, Fun, Acc, {Lower, Upper}, Order)
    end;
foldl([Path|Tail], Fun, Acc, {Lower, Upper}, Order) when Path < Lower ->
    case lists:prefix(Path, Lower) of
        true ->
            case file:list_dir(Path) of
                {ok, Paths} ->
                    foldl([filename:join(Path, F) || F <- Order(Paths)] ++ Tail, Fun, Acc, {Lower, Upper}, Order);
                _ ->
                    foldl(Tail, Fun, Acc, {Lower, Upper}, Order)
            end;
        false ->
            foldl(Tail, Fun, Acc, {Lower, Upper}, Order)
    end.

foldr(Tree, Fun, Acc) ->
    foldr(Tree, Fun, Acc, {undefined, undefined}).

foldr(Tree, Fun, Acc, {_, _} = Bounds) ->
    foldr(Tree, Fun, Acc, Bounds, fun (L) -> lists:reverse(lists:usort(L)) end);
foldr(Tree, Fun, Acc, Order) when is_function(Order) ->
    foldr(Tree, Fun, Acc, {undefined, undefined}, Order).

foldr(Path, Fun, Acc, Bounds, Order) when is_binary(Path) ->
    foldr([binary_to_list(Path)], Fun, Acc, Bounds, Order);
foldr([C|_] = Path, Fun, Acc, Bounds, Order) when is_integer(C) ->
    foldr([Path], Fun, Acc, Bounds, Order);
foldr([], _, Acc, _, _) ->
    Acc;
foldr([Path|Tail], Fun, Acc, {Upper, Lower}, Order) when Lower =/= undefined, Path < Lower ->
    case lists:prefix(Path, Lower) of
        true ->
            case file:list_dir(Path) of
                {ok, Paths} ->
                    foldr([filename:join(Path, F) || F <- Order(Paths)] ++ Tail, Fun, Acc, {Upper, Lower}, Order);
                _ ->
                    foldr(Tail, Fun, Acc, {Upper, Lower}, Order)
            end;
        false ->
            Acc
    end;
foldr([Path|Tail], Fun, Acc, {Upper, Lower}, Order) when Upper =:= undefined; Path < Upper ->
    case file:list_dir(Path) of
        {ok, Paths} ->
            foldr([filename:join(Path, F) || F <- Order(Paths)] ++ Tail, Fun, Acc, {Upper, Lower}, Order);
        {error, enotdir} ->
            foldr(Tail, Fun, Fun(Path, Acc), {Upper, Lower}, Order);
        {error, enoent} ->
            foldr(Tail, Fun, Acc, {Upper, Lower}, Order)
    end;
foldr([Path|Tail], Fun, Acc, {Upper, Lower}, Order) when Path >= Upper ->
    foldr(Tail, Fun, Acc, {Upper, Lower}, Order).

head(Tree) ->
    head(Tree, fun lists:usort/1).

head(Tree, Order) ->
    head(Tree, Order, fun (_) -> true end).

head(Path, Order, Filter) when is_binary(Path) ->
    head([Path], Order, Filter);
head([C|_] = Path, Order, Filter) when is_integer(C) ->
    head([Path], Order, Filter);
head([], _, _) ->
    undefined;
head([Path|Tail], Order, Filter) ->
    case filelib:is_regular(Path) of
        true ->
            Path;
        false ->
            case file:list_dir(Path) of
                {ok, [_|_] = Paths} ->
                    head([filename:join(Path, F) || F <- Order(Paths), Filter(F)] ++ Tail, Order, Filter);
                _ ->
                    head(Tail, Order, Filter)
            end
    end.

last(Tree) ->
    last(Tree, fun lists:usort/1).

last(Tree, Order) ->
    last(Tree, Order, fun (_) -> true end).

last(Tree, Order, Filter) ->
    head(Tree, fun (L) -> lists:reverse(Order(L)) end, Filter).
