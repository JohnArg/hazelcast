package discovery.common;

/**
 * Used to specify an {@link discovery.common.api.DiscoveryApi DiscoveryApi} operation.
 */
public class DiscoveryOperationType {
    // using final ints instead of enum, in order to be able to use in switch statements
    public static final int REGISTER_SERVER = 1;
    public static final int UNREGISTER_SERVER = 2;
    public static final int GET_SERVERS = 3;
}
