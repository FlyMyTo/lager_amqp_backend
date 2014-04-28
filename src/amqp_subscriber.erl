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

-record(state_await_conf, { config_ready_msg, init }).
-record(state, {channel,
		connection, 
		sub,
                consumer_tag,
		event_handler,
		buffer}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RoutingKey, EventHandler) 
  when is_binary(RoutingKey), 
       ( is_atom(EventHandler) orelse is_function(EventHandler, 1))  ->
    %ServerName = binary_to_atom(RoutingKey,latin1),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [RoutingKey, EventHandler], []).

notify(EventHandler, Event) when is_atom(EventHandler) ->
    EventHandler:notify(Event);
notify(EventHandler, Event) when is_function(EventHandler, 1) ->
    EventHandler(Event);
notify(Bad, _) ->
    exit({bad_event_handler, Bad}).

start_link(RoutingKey) when is_binary(RoutingKey) ->
    gen_server:start_link(
      {local, ?MODULE}, 
      ?MODULE, 
      [RoutingKey, fun(Evts) ->
			   [begin
				Msg={bigwig_trace, Evt},
				bigwig_pubsubhub:notify(Msg)
			    end || Evt <- Evts ],
			   ok
		   end ], []);

start_link(_RoutingKey) ->
    io:format("RoutingKey should be binary type").

cast(Msg) ->
    gen_server:cast(?MODULE,{Msg}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

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


init([RoutingKey, EventHandler]) ->
    LagerEnv = case application:get_all_env(lager) of
		   undefined -> [];
		   Env -> Env
	       end,
    HandlerConf = config_val(handlers, LagerEnv, []),
    Params      = config_val(lager_amqp_backend, HandlerConf, []),
    ConfMod     = config_val(amqp_conf_mod,  Params, ?MODULE),
    GetEnv      = if ConfMod == ?MODULE -> 
			  fun(Var) -> config_val(Var, Params) end;
		      true ->
			  fun(Var) -> ConfMod:get_env(lager_amqp_backend, Var) end
		  end,
    Init = fun() ->
	    Exchange   = to_bin(GetEnv(exchange)),
	    AmqpParams = #amqp_params_network {
			    username       = to_bin(GetEnv(user) ),
			    password       = to_bin(GetEnv(pass) ),
			    virtual_host   = to_bin(GetEnv(vhost)),
			    host           = to_str(GetEnv(host) ),
			    port           = to_int(GetEnv(port) )
			   },

	    {ok, Connection} =
		amqp_connection:start(AmqpParams),
	    {ok, Channel} = amqp_connection:open_channel(Connection),
	    #'exchange.declare_ok'{} = amqp_channel:call(
					 Channel, #'exchange.declare'{ 
						     exchange = Exchange, 
						     type = <<"topic">> }),
	    %% Declare a queue
	    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, #'queue.declare'{}),
	    Binding = #'queue.bind'{
			 queue = Q, exchange = Exchange, 
			 routing_key = RoutingKey},
	    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
	    Sub = #'basic.consume'{queue = Q},
	    %% Subscribe the channel and consume the message
	    Consumer = self(),
	    #'basic.consume_ok'{consumer_tag = Tag} = 
		       amqp_channel:subscribe(Channel, Sub, Consumer),
	    %% Start timer to flush each second
	    {ok, _} = timer:send_interval(1000, flush),
	    #state{channel       = Channel, 
		   connection    = Connection, 
		   sub           = Sub, 
		   consumer_tag  = Tag, 
		   event_handler = EventHandler,
		   buffer        = []}
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

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({unsubscribe}, State) ->
    io:format("unsubscribe begin ~n"),
    Consumer_tag = State#state.consumer_tag,
    Channel = State#state.channel,
    Connection = State#state.connection,
    Method = #'basic.cancel'{consumer_tag = Consumer_tag},
    #'basic.cancel_ok'{consumer_tag = _} = amqp_channel:call(Channel, Method),
    amqp_channel:close(Channel),
    amqp_channel:close(Connection),
    {noreply, State};
handle_cast({subscribe}, State) ->
     Channel=State#state.channel,
     Sub=State#state.sub,
     Consumer=self(),
     io:format("subscribe again"),
     #'basic.consume_ok'{consumer_tag = Consumer_tag} = amqp_channel:subscribe(Channel, Sub, Consumer),
     {noreply,#state{consumer_tag=Consumer_tag}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Msg, #state_await_conf{ 
		    config_ready_msg = Msg,
		    init             = InitFun } = _State) ->
    lager:log(debug, [], "Config ready~n", []),
    {noreply, InitFun()};
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{ delivery_tag = Tag}, {_, _, Message} = _Content } , 
	    #state{ buffer = Events } = State) ->
    lager:log(debug, [], "RCVD: ~ts~n", [Message]),
    Events1 = [ {Tag, Message} | Events],
    {noreply, if length(Events1) > 1000 ->
		      flush(State);
		 true -> State#state{ buffer = Events1 }
	      end };
handle_info(flush, State) ->
    {noreply, flush(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

flush(#state{ buffer = [] } = State) ->
    State;
flush(#state{channel       = Channel, 
	     event_handler = EventHandler,
	     buffer        = Events} = State) ->
    {Tags, Messages} = lists:unzip(lists:reverse(Events)),
    try
	notify(EventHandler, Messages),
	[ amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}) || Tag <- Tags ],
	lager:log(debug, [], "Flushed ~p events", [length(Events)]),
	State#state{ buffer = [] }
    catch
	_:Reason ->
	    lager:log(critical, [], "Failed to notify handler ~p due to ~p~n", [EventHandler, Reason]),
	    State
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

config_val(C, Params) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ -> exit({env_var_not_configured, C})
  end.

config_val(C, Params, Default) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ -> Default
  end.
