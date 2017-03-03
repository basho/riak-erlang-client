.PHONY: all lint clean compile deps distclean release docs

PROJDIR := $(realpath $(CURDIR))
REBAR ?= $(PROJDIR)/rebar

all: deps compile

lint: xref dialyzer

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps

release: compile
ifeq ($(VERSION),)
	$(error VERSION must be set to build a release and deploy this package)
endif
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	echo "==> Tagging version $(VERSION)"
	$(PROJDIR)/tools/build/publish $(VERSION) master validate
	echo "$(VERSION)" > $(PROJDIR)/VERSION
	git add --force $(PROJDIR)/VERSION
	git commit --message="riak-erlang-client $(VERSION)"
	git push
	git tag --sign -a "$(VERSION)" -m "riak-erlang-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	git push --tags
	$(PROJDIR)/tools/build/publish $(VERSION) master 'Riak Erlang Client' 'riak-erlang-client'
	mix deps.get
	mix hex.publish


DIALYZER_APPS = kernel stdlib sasl erts eunit ssl tools crypto \
       inets public_key syntax_tools compiler

include tools.mk
