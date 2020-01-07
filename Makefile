.PHONY: all lint clean compile deps distclean release docs

REBAR=./rebar3

all: deps compile

lint: xref dialyzer

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean --all

shell:
	$(REBAR) shell

release: compile
ifeq ($(VERSION),)
	$(error VERSION must be set to build a release and deploy this package)
endif
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@echo "==> Tagging version $(VERSION)"
	# NB: Erlang client version strings do NOT start with 'v'. Le Sigh.
	# validate VERSION and allow pre-releases
	@./tools/build/publish $(VERSION) master validate
	@git tag --sign -a "$(VERSION)" -m "riak-erlang-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	@git push --tags
	@./tools/build/publish $(VERSION) master 'Riak Erlang Client' 'riak-erlang-client'


DIALYZER_APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler

include tools.mk
