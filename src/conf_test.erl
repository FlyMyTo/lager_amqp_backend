%%%-------------------------------------------------------------------
%%% @author Vorobyov Vyacheslav <vjache@gmail.com>
%%% @copyright (C) 2014, Vorobyov Vyacheslav
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(conf_test).

%% API
-export([get_env/2, config_ready_msg/0]).

%%%===================================================================
%%% API
%%%===================================================================
config_ready_msg() ->
    self() ! ?MODULE,
    ?MODULE.

get_env(_,VarName) ->
    proplists:get_value(
      VarName,
      [{routing_key, [<<"error">>]},                   
       {level,    info},
       {exchange, "lager_amqp_backend"},
       {user,     "guest"},
       {pass,     "guest"},
       {vhost,    "/"},
       {host,     "linode.talkan.name"},
       {port,     5672}]).
	
%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
