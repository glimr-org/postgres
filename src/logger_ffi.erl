%% Logger configuration FFI for tests
-module(logger_ffi).

-export([suppress_supervisor_reports/0]).

%% Suppress pgo pool supervisor shutdown reports (expected during pool cleanup)
suppress_supervisor_reports() ->
    logger:add_primary_filter(
        suppress_pgo_supervisor_shutdown,
        {fun filter_pgo_supervisor_reports/2, []}
    ),
    nil.

%% Only filter pgo_pool_sup shutdown errors, let other supervisor reports through
filter_pgo_supervisor_reports(#{msg := {report, #{label := {supervisor, _}, report := Report}}}, _) ->
    Supervisor = proplists:get_value(supervisor, Report),
    ErrorContext = proplists:get_value(errorContext, Report),
    case {Supervisor, ErrorContext} of
        {{_, pgo_pool_sup}, shutdown_error} -> stop;
        _ -> ignore
    end;
filter_pgo_supervisor_reports(_, _) ->
    ignore.
