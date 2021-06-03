# RDMA In The CP Subsystem

## RDMA For Raft RPCs

### Configuring Hazelcast Servers
The CP subsystem can optionally use RDMA for
Raft RPCs, simply by using an 
<i>rdma.properties</i> configuration file.
To do this, you have to :

    1. Copy the <i>rdma-example.properties</i> file 
        in <i>hazelcast/hazelcast/src/main/resources/</i> and
        paste it in the same folder with the name <i>rdma.properties</i>.
    2. Change configuration parameters inside <i>rdma.properties</i>
        according to your needs. There are comments explaining each
        configuration parameter.

### Using The RDMA Discovery Service
Additionally, before running Hazelcast servers that use RDMA for the CP
subsystem, you need to run a discovery service for the servers to find
each other.
The discovery service is implemented in the <i>rdma-server-discovery</i> 
maven module.
The service must be configured similarly to the Hazelcast Servers for 
RDMA communications, by changing configuration parameters in the
<i>discovery.properties</i> file inside
<i>hazelcast/rdma-server-discovery/src/main/resources/</i>.

The RDMA discovery service can be started by running the DiscoveryServiceStarter
class in
<i>hazelcast/rdma-server-discovery/src/main/java/discovery/service/</i>.

