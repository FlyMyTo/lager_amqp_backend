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

-record(state, {name,
                level,
                exchange,
                params,
                routing_key,
		formatter_config
               }).
init({RoutingKey, Level, Host}) when is_binary(RoutingKey), is_atom(Level) ->
    init([{routing_key, RoutingKey}, {level, Level}, {amqp_host, Host}]);

init(Params) when is_list(Params) ->
  
    Name  = config_val(name, Params, ?MODULE),
    Level = lager_util:level_to_num(config_val(level, Params, debug)),
    Exchange = config_val(exchange, Params, list_to_binary(atom_to_list(?MODULE))),
    RoutingKey  = config_val(routing_key, Params, undefined),

    AmqpParams = #amqp_params_network {
      username       = config_val(amqp_user, Params, <<"guest">>),
      password       = config_val(amqp_pass, Params, <<"guest">>),
      virtual_host   = config_val(amqp_vhost, Params, <<"/">>),
      host           = config_val(amqp_host, Params, "127.0.0.1"),
      port           = config_val(amqp_port, Params, 5672)
     },
  
    {ok, Channel} = amqp_channel(AmqpParams),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{ exchange = Exchange, 
                                                                               type = <<"topic">> }),
  
    {ok, #state{ name  = Name,
                 routing_key = RoutingKey,
                 level = Level, 
                 exchange = Exchange,
                 params = AmqpParams,
		 formatter_config = {_ContType, _Conf} = config_val(formatter_config, Params, {<<"application/json">>, undefined} )
               }}.

handle_call({set_loglevel, Level}, #state{ name = _Name } = State) ->
    % io:format("Changed loglevel of ~s to ~p~n", [Name, Level]),
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
    Meta = [ E || E={A,B} <- [ case M of
				   {Key, Val}           -> {to_atom(Key), to_hr_bin(Val) };
						%Pid when is_pid(Pid) -> {pid, Pid};
						%Atom when is_atom(Atom) -> {tag, to_hr_bin(Atom)}
				   _ -> skip
			       end || M <- lager_msg:metadata(LagerMesg) ], is_atom(A), is_binary(B)],
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


send(#state{ name = Name, exchange = Exchange, routing_key = RK, 
	     formatter_config = {ContType, _Conf} } = State, Node, Level, MessageBody, Channel) ->
    RoutingKey = case RK of
                     undefined -> routing_key(Node, Name, Level);
                     _ -> RK
                 end,
    Publish = #'basic.publish'{ exchange = Exchange, routing_key = RoutingKey },
    Props = #'P_basic'{ content_type = ContType },
    Msg = #amqp_msg{ payload = MessageBody, props = Props },
    
    % io:format("message: ~p~n", [Msg]),
    amqp_channel:cast(Channel, Publish, Msg),
    
    State.

routing_key(Node, Name, Level) ->
    RkPrefix = atom_to_list(lager_util:num_to_level(Level)),
    RoutingKey =  case Name of
                      []   ->  string:join([Node, RkPrefix], ".");
                      Name ->  string:join([Node, Name, RkPrefix], ".")
                  end,
    list_to_binary(RoutingKey).

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
  
