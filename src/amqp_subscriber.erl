%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2013, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 25 Oct 2013 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(amqp_subscriber).

-behaviour(gen_server).

%% API
-export([ start_link/1,
	  start_link/2,
	  cast/1 ]).

%% gen_server callbacks
-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2,
         terminate/2, 
         code_change/3]).

-define(SERVER, ?MODULE). 
-include_lib("amqp_client/include/amqp_client.hrl").


-record(state, {channel,
		connection, 
		sub,
                consumer_tag,
		event_handler }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RoutingKey, EventHandler) when is_binary(RoutingKey), 
					  ( is_atom(EventHandler) orelse is_function(EventHandler, 1))  ->
    ServerName = binary_to_atom(RoutingKey,latin1),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [RoutingKey, EventHandler], []).

notify(EventHandler, Event) when is_atom(EventHandler) ->
    EventHandler:notify(Event);
notify(EventHandler, Event) when is_function(EventHandler, 1) ->
    EventHandler(Event).


start_link(RoutingKey) when is_binary(RoutingKey) ->
    ServerName = binary_to_atom(RoutingKey,latin1),
    gen_server:start_link(
      {local, ?MODULE}, 
      ?MODULE, 
      [RoutingKey, fun(Evt) -> 
			   Msg={bigwig_trace, Evt},
			   bigwig_pubsubhub:notify(Msg)
		   end ], []);

start_link(_RoutingKey) ->
    io:format("RoutingKey should be binary type").

cast(Msg) ->
    gen_server:cast(?MODULE,{Msg}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([RoutingKey, EventHandler]) ->
    LagerEnv = case application:get_all_env(lager) of
                   undefined -> [];
                   Env -> Env
               end,
    HandlerConf = config_val(handlers, LagerEnv, []),
    Params = config_val(lager_amqp_backend, HandlerConf, []),
    
    Exchange = config_val(exchange, Params, <<"lager_amqp_backend">>),
    AmqpParams = #amqp_params_network {
      username       = config_val(amqp_user, Params, <<"guest">>),
      password       = config_val(amqp_pass, Params, <<"guest">>),
      virtual_host   = config_val(amqp_vhost, Params, <<"/">>),
      host           = config_val(amqp_host, Params, "localhost"),
      port           = config_val(amqp_port, Params, 5672)
     },

    {ok, Connection} =
        amqp_connection:start(AmqpParams),
    {ok, Channel} = amqp_connection:open_channel(Connection),

    %{ok, Channel} = amqp_channel(AmqpParams),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{ exchange = Exchange, 
                                                                               type = <<"topic">> }),

    %% Declare a queue
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, #'queue.declare'{}),
    Binding = #'queue.bind'{queue = Q, exchange = Exchange, routing_key = RoutingKey},
     #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    Sub = #'basic.consume'{queue = Q},
    % Subscribe the channel and consume the message
    Consumer = self(),
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Sub, Consumer),
    io:format("init start ~n"),
    io:format("channel is ~p~n",[Channel]),
    io:format("consumer_tag is ~p~n",[Tag]),
    io:format("init over ~n"),
    {ok, #state{channel = Channel, 
		connection = Connection, 
		sub = Sub, 
		consumer_tag = Tag, 
		event_handler = EventHandler}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({unsubscribe}, State) ->
    io:format("unsubscribe begin ~n"),
    Consumer_tag = State#state.consumer_tag,
    Channel = State#state.channel,
    Connection = State#state.connection,
    Method = #'basic.cancel'{consumer_tag = Consumer_tag},
    #'basic.cancel_ok'{consumer_tag = Consumer_tag1} = amqp_channel:call(Channel, Method),
    amqp_channel:close(Channel),
    amqp_channel:close(Connection),
    io:format("channel is ~p~n",[Channel]),
    io:format("consumer_tag is ~p~n",[Consumer_tag1]),
    io:format("unsubscribe over ~n"),
    {noreply, State};
handle_cast({subscribe}, State) ->
     Channel=State#state.channel,
     Sub=State#state.sub,
     Consumer=self(),
     io:format("subscribe again"),
     #'basic.consume_ok'{consumer_tag = Consumer_tag} = amqp_channel:subscribe(Channel, Sub, Consumer),
     io:format("channel is ~p~n",[Channel]),
     io:format("consumer_tag is ~p~n",[Consumer_tag]),
     io:format("subscribe again over"),
     {noreply,#state{consumer_tag=Consumer_tag}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{ delivery_tag = Tag}, 
	     {_, _, Message} = _Content }, 
	    #state{channel = Channel, 
		   event_handler = EventHandler} = State) ->
    io:format("> ~ts~n", [Message]),
    notify(EventHandler, Message),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


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


config_val(C, Params, Default) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ -> Default
  end.
