.PHONY: all clean compile deps distclean

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps


DIALYZER_APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler

include tools.mk
