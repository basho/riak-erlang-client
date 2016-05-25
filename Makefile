.PHONY: all clean compile deps distclean release docs

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

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
	@bash ./build/publish $(VERSION) validate
	@git tag --sign -a "$(VERSION)" -m "riak-erlang-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	@git push --tags
	@bash ./build/publish $(VERSION)


DIALYZER_APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler

include tools.mk
