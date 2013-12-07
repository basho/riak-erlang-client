.PHONY: deps doc

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
	./rebar skip_deps=true eunit

APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler
COMBO_PLT = $(HOME)/.riak_combo_dialyzer_plt

check_plt: all
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

build_plt: all
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

dialyzer: compile
	@dialyzer --plt $(COMBO_PLT) -Wno_return -c ebin

doc :
	@./rebar doc skip_deps=true

