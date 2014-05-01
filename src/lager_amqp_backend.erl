%% Copyright (c) 2011 by Jon Brisbin. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

-module(lager_amqp_backend).
-behaviour(gen_event).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([init/1, 
         handle_call/2, 
         handle_event/2, 
         handle_info/2, 
         terminate/2,
         code_change/3
        ]).
-export([test/0]).

-record(state, {level,
                exchange,
                params,
                routing_key,
		formatter_config,
		vars,
		amqp_channel, 
		amqp_connection
               }).
-record(state_await_conf, { id, config_ready_msg, init }).

-define(LOG_ERROR(Rep), 
	error_logger:error_report(Rep)).
-define(LOG_WARN(Rep), 
	error_logger:warning_report(Rep)).
-define(LOG_INFO(Rep), 
	error_logger:info_report(Rep)).

to_bin(B) when is_binary(B) ->
    B;
to_bin(L) when is_list(L)   ->
    list_to_binary(L);
to_bin(A) when is_atom(A)   ->
    atom_to_binary(A, latin1).

to_str(B) when is_binary(B) ->
    binary_to_list(B);
to_str(L) when is_list(L)   ->
    L;
to_str(A) when is_atom(A)   ->
    atom_to_list(A).

to_int(I) when is_integer(I) ->
    I;
to_int(B) when is_binary(B)  ->
    list_to_integer(binary_to_list(B));
to_int(L) when is_list(L)    ->
    list_to_integer(L).

init(Params) when is_list(Params) ->
    ConfMod     = config_val(conf_mod,  Params, ?MODULE),
    GetEnv      = if ConfMod == ?MODULE -> 
			  fun(Var) -> config_val(Var, Params) end;
		     true ->
			  fun(Var) -> ConfMod:get_env(lager_amqp_backend, Var) end
		  end,
    Me = make_ref(),
    Init = 
	fun() -> 
		Level         = lager_util:level_to_num(GetEnv(level)),
		Exchange      = to_bin(GetEnv(exchange)),
		RoutingKeyFmt = case GetEnv(routing_key) of
				    undefined -> [node_host, node_name, level];
				    V         -> V
				end,
		AmqpParams = #amqp_params_network {
				username       = to_bin(GetEnv(user) ),
				password       = to_bin(GetEnv(pass) ),
				virtual_host   = to_bin(GetEnv(vhost)),
				host           = to_str(GetEnv(host) ),
				port           = to_int(GetEnv(port) ),
				heartbeat      = 10,
				connection_timeout = 10000
			       },
		Vars = mk_vars(),
		self() ! {connect, Me, 0, 
			  fun() ->
				  connect(
				    #state{
				       routing_key      = RoutingKeyFmt,
				       level            = Level, 
				       exchange         = Exchange,
				       params           = AmqpParams,
				       formatter_config = {<<"application/json">>, undefined},
				       vars             = Vars
				      })
			  end}
	end,

    if ConfMod == ?MODULE -> 
	    CMsg = config_ready_msg,
	    self() ! CMsg;
       true -> 
	    CMsg = ConfMod:config_ready_msg()
    end,
    {ok, #state_await_conf{
	    id = Me, 
	    config_ready_msg = CMsg,
	    init = Init } }.

mk_vars() ->
    NodeStr = atom_to_list(node()),
    [NodeName, NodeHost] = string:tokens(NodeStr, "@"),
    [{node_name, NodeName},
     {node_host, NodeHost},
     {node, NodeStr}].

handle_call({set_loglevel, Level}, #state{ } = State) ->
    {ok, ok, State#state{ level=lager_util:level_to_num(Level) }};
    
handle_call(get_loglevel, #state{ level = Level } = State) ->
    {ok, Level, State};
    
handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({log,  Message}, #state{routing_key = RoutingKey, level = L } = State) ->
    case lager_util:is_loggable(Message, L, {lager_amqp_backend, RoutingKey}) of
        true ->
            {ok, 
            log(State, L, Message)};
        false ->
            {ok, State}
    end;
  
handle_event(_Event, State) ->
    {ok, State}.

handle_info(Msg, #state_await_conf{ 
		    config_ready_msg = Msg,
		    init             = InitFun } = State) ->
    InitFun(),
    {ok, State};
handle_info({connect, Id, N, ConnectFun}, 
	    #state_await_conf{ id = Id } = State) ->
    State1 = 
	try ConnectFun()
	catch 
	    _:Reason ->
		NextInterval = get_interval(N),
		?LOG_WARN(
		   ["Lager AMQP backend connect attempt failed.", 
		    {reason, Reason},
		    {reconnect_after, NextInterval}]),
		timer:send_after(NextInterval, {connect, Id, N + 1, ConnectFun}),
		State
	end,
    {ok, State1};
handle_info({'DOWN', _Mref, process, ChanPid, Reason}, 
 	    #state{ amqp_channel = ChanPid} = State) ->
    ?LOG_ERROR(["Lager AMQP backend have lost channel.",
		{reason, Reason}]),
    exit(amqp_channel_down),
     {ok, State};
handle_info({'DOWN', _Mref, process, ConnPid, Reason}, 
	    #state{ amqp_connection = ConnPid} = State) ->
    ?LOG_ERROR(["Lager AMQP backend have lost connecton.",
		{reason, Reason}]),
    exit(amqp_connection_down),
    {ok, State};
handle_info(_Inf, State) ->
    io:format("INFO >>>>>>> ~p~n",[_Inf]),
    {ok, State}.

get_interval(N) ->
         5000 *
        (case N of
             0 -> 1;
	     1 -> 1;
             2 -> 2;
             3 -> 4;
             4 -> 8;
             5 -> 16;
             6 -> 24;
             _ -> 30
         end).


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_utc_datetime2zulu({{Year, Month, Day}, {Hour, Min, Sec}}, Mls) ->
    iolist_to_binary(
      io_lib:format(
	"~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0w.~.3.0wZ",
	[Year, Month, Day, Hour, Min, Sec, Mls] )).

format_event(LagerMesg, #state{formatter_config = {<<"text/plain">>, Conf} })->
    iolist_to_binary(lager_default_formatter:format(LagerMesg, Conf));
format_event(LagerMesg, #state{formatter_config = {<<"application/json">>, _Conf} }) ->
    DTBin  = format_utc_datetime2zulu(calendar:now_to_universal_time(os:timestamp()),0),
    SevBin = atom_to_binary(lager_msg:severity(LagerMesg),latin1),
    MsgBin = unicode:characters_to_binary(lists:flatten(lager_msg:message(LagerMesg))),
    Meta0 = [ E || E={A,B} <- [ case M of
				   {Key, Val}           -> {to_atom(Key), to_hr_bin(Val) };
						%Pid when is_pid(Pid) -> {pid, Pid};
						%Atom when is_atom(Atom) -> {tag, to_hr_bin(Atom)}
				   _ -> skip
			       end || M <- lager_msg:metadata(LagerMesg) ], 
		   is_atom(A), is_binary(B)],
    Meta = 
	case lists:keymember(node, 1, Meta0) of
	    false -> [{node, to_hr_bin(node())} | Meta0 ];
	    true -> Meta0
	end,
    jsonx:encode( {lists:flatten(
		     [{timestamp, DTBin}, 
		      {severity, SevBin},
		      {meta, {Meta}},
		      {message, MsgBin}])} ).

to_hr_bin(B) when is_binary(B) ->
    B;
to_hr_bin(L) when is_list(L) andalso is_integer(hd(L)) ->
    list_to_binary(L);
to_hr_bin(A) when is_atom(A) ->
    atom_to_binary(A,latin1);
to_hr_bin(Any) ->
    iolist_to_binary(io_lib:format("~p", [Any])).

to_atom(A) when is_atom(A) ->
    A;
to_atom(L) when is_list(L) andalso is_integer(hd(L)) ->
    list_to_atom(L);
to_atom(B) when is_binary(B) ->
    binary_to_atom(B, latin1).





log(#state{} = State, Level, Message) ->
    send(State, Level, format_event(Message, State)).


send(#state{ exchange         = Exchange, 
	     routing_key      = RoutingKeyFmt, 
	     formatter_config = {ContType, _Conf},
	     vars             = Vars,
	     amqp_channel     = Channel } = State, 
     LevelNum, MessageBody) ->
    Level      = atom_to_list(lager_util:num_to_level(LevelNum)),
    RoutingKey = format_routing_key(RoutingKeyFmt, [{level, Level} | Vars]),
    Publish    = #'basic.publish'{ exchange = Exchange, routing_key = RoutingKey },
    Props      = #'P_basic'{ content_type = ContType },
    Msg        = #amqp_msg{ payload = MessageBody, props = Props },
    amqp_channel:cast(Channel, Publish, Msg),
    State.

format_routing_key(RoutingKeyFmt, _Env) when is_binary(RoutingKeyFmt) ->
    RoutingKeyFmt;
format_routing_key(RoutingKeyFmt, Env) ->
    RoutingKey = 
	string:join(
	  [ case Elem of
		{OSVarNameA, Default} when is_atom(OSVarNameA) ->
		    [$$ | OSVarName] = atom_to_list(OSVarNameA),
		    case os:getenv(OSVarName) of
			false -> to_str(Default);
			Val   -> Val
		    end;
		_ when is_atom(Elem) ->
		    case proplists:get_value(Elem, Env) of
			undefined ->
			    case atom_to_list(Elem) of
				[$$ | OSVarName] ->
				    Val = os:getenv(OSVarName),
				    Val == false andalso exit({var_undef, Elem}),
				    Val;
				_ -> exit({var_undef, Elem})
			    end;
			Val -> to_str(Val)
		    end;
	       _ when is_binary(Elem) orelse 
		      is_list(Elem)   orelse 
		      is_atom(Elem) -> to_str(Elem)
	    end || Elem <- RoutingKeyFmt], "."),
    list_to_binary(RoutingKey).

config_val(C, Params) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ -> undefined
  end.

config_val(C, Params, Default) ->
    case lists:keyfind(C, 1, Params) of
      {C, V} -> V;
        _ -> Default
    end.


connect(#state{ params = AmqpParams, exchange = Exchange } = State) ->
    %% 1. Create connection & channel
    {ok, Connection } = amqp_connection:start(AmqpParams),
    {ok, Channel    } = amqp_connection:open_channel(Connection),
    %% %% 2. Create exchange
    #'exchange.declare_ok'{} = 
    	amqp_channel:call(
    	  Channel, 
    	  #'exchange.declare'{
    	     exchange = Exchange, 
    	     type = <<"topic">>,
	     durable = true }),
    %% %% 3. Create default queue & bind it to exchange
    %% #'queue.declare_ok'{} =
    %% 	amqp_channel:call(Channel, #'queue.declare'{queue=Exchange}),
    %% #'queue.bind_ok'{} =
    %% 	amqp_channel:call(Channel, #'queue.bind'{queue=Exchange,exchange=Exchange}),
    %% 4. Start monitor channel & connection pids
%%    erlang:monitor(process, Channel),
    erlang:monitor(process, Connection),
    State#state{ amqp_channel = Channel, amqp_connection = Connection }.
  
test() ->
  application:load(lager),
  %%application:set_env(lager, handlers, [{lager_console_backend, debug}, {lager_amqp_backend, []}]),
  %%application:set_env(lager, error_logger_redirect, false),
  application:start(lager),
  lager:log(info, self(), "Test INFO message"),
  lager:log(debug, self(), "Test DEBUG message"),
  lager:log(error, self(), "Test ERROR message").
  
