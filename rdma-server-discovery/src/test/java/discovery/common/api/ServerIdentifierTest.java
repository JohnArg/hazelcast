package discovery.common.api;

import discovery.common.api.ServerIdentifier;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ServerIdentifierTest {

    @Test
    @Tag("Equality")
    @DisplayName("Testing equals correctness.")
    public void equalityTest(){
        Set<ServerIdentifier> identifiers;
        // addresses null - different objects
        ServerIdentifier identifier = new ServerIdentifier();
        ServerIdentifier identifierCopy = new ServerIdentifier();
        assertTrue(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertFalse(identifiers.add(identifierCopy));
        // addresses null - same objects
        identifierCopy = identifier;
        assertTrue(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertFalse(identifiers.add(identifierCopy));

        // Different objects - Same IPs/ports --------------
        // rdma address set - tcp address null
        identifierCopy = new ServerIdentifier();
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifierCopy.setRdmaAddress(new InetSocketAddress(3000));
        assertTrue(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertFalse(identifiers.add(identifierCopy));
        // both addresses set
        identifier.setTcpAddress(new InetSocketAddress(4000));
        identifierCopy.setTcpAddress(new InetSocketAddress(4000));
        assertTrue(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertFalse(identifiers.add(identifierCopy));
        // rdma address null - tcp address set
        identifier.setRdmaAddress(null);
        identifierCopy.setRdmaAddress(null);
        assertTrue(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertFalse(identifiers.add(identifierCopy));
        // Different objects - Different IPs/ports --------------
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifierCopy.setRdmaAddress(new InetSocketAddress(6000));
        identifier.setTcpAddress(new InetSocketAddress(4000));
        identifierCopy.setTcpAddress(new InetSocketAddress(4000));
        assertFalse(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertTrue(identifiers.add(identifierCopy));

        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifierCopy.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(new InetSocketAddress(4000));
        identifierCopy.setTcpAddress(new InetSocketAddress(6000));
        assertFalse(identifier.equals(identifierCopy));
        identifiers = new HashSet<>();
        assertTrue(identifiers.add(identifier));
        assertTrue(identifiers.add(identifierCopy));
    }

}
