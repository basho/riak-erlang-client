PROJDIR = $(realpath $(CURDIR)/..)

TOOLS_DIR = $(PROJDIR)/tools/devrel
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

export RIAK_TEST_HOST_1 = 127.0.0.1
export RIAK_TEST_NODE_1 = dev1@127.0.0.1
export RIAK_TEST_PBC_1 = 8087

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true false

compile:
	@${MAKE} -C $(PROJDIR) compile

lint:
	@${MAKE} -C $(PROJDIR) xref dialyzer

test:
	@${MAKE} -C $(PROJDIR) test
