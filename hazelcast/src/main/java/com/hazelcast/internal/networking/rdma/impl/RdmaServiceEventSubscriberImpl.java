package com.hazelcast.internal.networking.rdma.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.internal.networking.rdma.RdmaService;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.spi.impl.NodeEngine;

/**
 * Will receive and handle CP related events on behalf of an {@link RdmaService}.
 */
public class RdmaServiceEventSubscriberImpl implements CPMembershipListener {

    private RdmaService service;
    private Member localMember;
    private final RdmaLogger logger;

    public RdmaServiceEventSubscriberImpl(RdmaService service, NodeEngine engine, Member localMember){
        this.service = service;
        logger = new RdmaLogger(engine.getLogger(RdmaServiceEventSubscriberImpl.class.getSimpleName()));
        this.localMember = localMember;
    }

    /* *******************************************
    *   CP events
    * *******************************************/

    @Override
    public void memberAdded(CPMembershipEvent event) {
        logger.info("Member added : "+ event.getMember().getUuid());
    }

    @Override
    public void memberRemoved(CPMembershipEvent event) {
        logger.info("Member added : "+ event.getMember().getUuid());
    }

}
