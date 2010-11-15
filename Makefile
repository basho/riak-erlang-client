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
	./rebar eunit

dialyzer: compile
	@dialyzer -Wno_return -c ebin

doc :
	@./rebar doc skip_deps=true

