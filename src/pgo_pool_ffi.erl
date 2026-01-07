%% FFI for PostgreSQL pool - returns closures that capture the pool handle
-module(pgo_pool_ffi).

-export([make_pool_ops/1]).

%% Create PoolOps from a pool name
%% PoolOps = {pool_ops, CheckoutFn, StopFn}
%% CheckoutFn = fun() -> {ok, {Conn, ReleaseFn}} | {error, Reason}
%% ReleaseFn = fun() -> nil
%% StopFn = fun() -> nil
make_pool_ops(Name) when is_atom(Name) ->
    CheckoutFn = fun() -> do_checkout(Name) end,
    StopFn = fun() -> stop_pool(Name), nil end,
    {pool_ops, CheckoutFn, StopFn}.

%% Internal checkout that returns {ok, {Conn, ReleaseFn}} or {error, Reason}
do_checkout(Name) ->
    case pgo:checkout(Name) of
        {ok, Ref, Conn} ->
            %% Wrap conn as pog's SingleConnection type
            SingleConn = {single_connection, Conn},
            ReleaseFn = fun() -> pgo:checkin(Ref, Conn), nil end,
            {ok, {SingleConn, ReleaseFn}};
        {error, Reason} ->
            {error, format_error(Reason)}
    end.

%% Stop a pool by name
stop_pool(Name) when is_atom(Name) ->
    case whereis(Name) of
        undefined ->
            nil;
        Pid ->
            supervisor:terminate_child(pgo_sup, Pid),
            nil
    end.

format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) when is_list(Reason) ->
    list_to_binary(Reason);
format_error(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
format_error(Reason) ->
    list_to_binary(io_lib:format("~p", [Reason])).
