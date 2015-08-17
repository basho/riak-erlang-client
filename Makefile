.PHONY: all clean compile upgrade-deps distclean

all: compile

compile:
	./rebar3 compile

upgrade-deps:
	./rebar3 upgrade

clean:
	./rebar3 clean

distclean: clean
	./rebar3 clean --all


DIALYZER_APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler

include tools.mk
