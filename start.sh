#! /bin/sh

env ERL_LIBS=deps erl -pa ebin -sname log_amqp -config etc/app.config -eval "application:ensure_all_started(lager)"
