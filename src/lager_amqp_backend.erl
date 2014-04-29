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
		vars
               }).
-record(state_await_conf, { config_ready_msg, init }).

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
    Init = 
	fun() -> 
		Level      = lager_util:level_to_num(GetEnv(level)),
		Exchange   = to_bin(GetEnv(exchange)),
		RoutingKeyFmt = case GetEnv(routing_key) of
				    undefined -> [node_host, node_name, level];
				    V         -> V
				end,
		AmqpParams = #amqp_params_network {
				username       = to_bin(GetEnv(user) ),
				password       = to_bin(GetEnv(pass) ),
				virtual_host   = to_bin(GetEnv(vhost)),
				host           = to_str(GetEnv(host) ),
				port           = to_int(GetEnv(port) )
			       },
		lager:log(info, [], "Lager AMQP params are: ~p~n", [AmqpParams]),
		{ok, Channel} = amqp_channel(AmqpParams),
		#'exchange.declare_ok'{} = amqp_channel:call(
					     Channel, #'exchange.declare'{
							 exchange = Exchange, 
							 type = <<"topic">> }),
		Vars = mk_vars(),
		#state{ routing_key      = RoutingKeyFmt,
			level            = Level, 
			exchange         = Exchange,
			params           = AmqpParams,
			formatter_config = {<<"application/json">>, undefined},
			vars = Vars
		      }
	end,

    if ConfMod == ?MODULE -> 
	    CMsg = config_ready_msg,
	    self() ! CMsg;
       true -> 
	    CMsg = ConfMod:config_ready_msg()
    end,
    {ok, #state_await_conf{ 
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
		    init             = InitFun } = _State) ->
    State1 = InitFun(),
    lager:log(info, [], "Config ready~n", []),
    {ok, State1};
handle_info(_Info, State) ->
    {ok, State}.

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
			       end || M <- lager_msg:metadata(LagerMesg) ], is_atom(A), is_binary(B)],
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





log(#state{params = AmqpParams } = State, Level, Message) ->
    case amqp_channel(AmqpParams) of
        {ok, Channel} ->	    
            send(State, atom_to_list(node()), Level, format_event(Message, State), Channel);
        _ ->
            State
    end.    


send(#state{ exchange         = Exchange, 
	     routing_key      = RoutingKeyFmt, 
	     formatter_config = {ContType, _Conf},
	     vars             = Vars} = State, 
     _Node, LevelNum, MessageBody, Channel) ->
    Level      = atom_to_list(lager_util:num_to_level(LevelNum)),
    RoutingKey = format_routing_key(RoutingKeyFmt, [{level, Level} | Vars]),
    Publish    = #'basic.publish'{ exchange = Exchange, routing_key = RoutingKey },
    Props      = #'P_basic'{ content_type = ContType },
    Msg        = #amqp_msg{ payload = MessageBody, props = Props },
%%    lager:log(debug, [], "SEND: RK = ~p, MSG = ~p", [RoutingKey, MessageBody]),
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

amqp_channel(AmqpParams) ->
    case maybe_new_pid({AmqpParams, connection},
                       fun() -> amqp_connection:start(AmqpParams) end) of
        {ok, Client} ->
            maybe_new_pid({AmqpParams, channel},
                          fun() -> amqp_connection:open_channel(Client) end);
        Error ->
	    lager:log(error, [], "Failed to establish AMQP channel. Reason = ~p. Params = ~p.",
		      [Error, AmqpParams]),
            Error
    end.

maybe_new_pid(Group, StartFun) ->
    case pg2:get_closest_pid(Group) of
        {error, {no_such_group, _}} ->
            pg2:create(Group),
            maybe_new_pid(Group, StartFun);
        {error, {no_process, _}} ->
            case StartFun() of
                {ok, Pid} ->
                    pg2:join(Group, Pid),
                    {ok, Pid};
                Error ->
                    Error
            end;
        Pid ->
            {ok, Pid}
    end.
  
test() ->
  application:load(lager),
  %%application:set_env(lager, handlers, [{lager_console_backend, debug}, {lager_amqp_backend, []}]),
  %%application:set_env(lager, error_logger_redirect, false),
  application:start(lager),
  lager:log(info, self(), "Test INFO message"),
  lager:log(debug, self(), "Test DEBUG message"),
  lager:log(error, self(), "Test ERROR message").
  
