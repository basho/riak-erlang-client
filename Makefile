.PHONY: deps

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean 
	./rebar delete-deps

test: 
	./rebar eunit

dialyzer: compile
	@dialyzer -Wno_return -c apps/riak/ebin


