package discovery.common.api;

import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Objects;

/**
 * Identifies a server by using a pair of RDMA and TCP address.
 * This is useful for an RDMA implementation that works alongside TCP.
 * Hazelcast transmits operations to a target address, which will be
 * a TCP address. So this class can associate the TCP address with
 * the RDMA address that the remote server is using.
 */
public class ServerIdentifier extends AbstractDataSerializer {
    private static final long serialVersionId = 1L;

    private InetSocketAddress rdmaAddress;
    private InetSocketAddress tcpAddress;

    public ServerIdentifier(){}

    public ServerIdentifier(InetSocketAddress rdmaAddress, InetSocketAddress tcpAddress) {
        this.rdmaAddress = rdmaAddress;
        this.tcpAddress = tcpAddress;
    }

    public InetSocketAddress getRdmaAddress() {
        return rdmaAddress;
    }

    public void setRdmaAddress(InetSocketAddress rdmaAddress) {
        this.rdmaAddress = rdmaAddress;
    }

    public InetSocketAddress getTcpAddress() {
        return tcpAddress;
    }

    public void setTcpAddress(InetSocketAddress tcpAddress) {
        this.tcpAddress = tcpAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerIdentifier that = (ServerIdentifier) o;
        return Objects.equals(rdmaAddress, that.rdmaAddress) && Objects.equals(tcpAddress, that.tcpAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rdmaAddress, tcpAddress);
    }

    /* *************************************************************
     * Serialization & Deserialization
     * ********************************************************** */

    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = workRequestProxy.getBuffer();
        try {
            // first write the serial version
            buffer.putLong(serialVersionId);
            // get the RDMA address first -----------------
            if (rdmaAddress == null) {
                buffer.putInt(-1);
            } else {
                byte[] addressBytes = rdmaAddress.getAddress().getAddress();
                // specify the number of bytes of the address
                buffer.putInt(addressBytes.length);
                // now put the address bytes
                buffer.put(addressBytes);
                // and finally put the port number
                buffer.putInt(rdmaAddress.getPort());
            }
            // now the TCP address -----------------
            if (tcpAddress == null) {
                buffer.putInt(-1);
            } else {
                byte[] addressBytes = tcpAddress.getAddress().getAddress();
                // specify the number of bytes of the address
                buffer.putInt(addressBytes.length);
                // now put the address bytes
                buffer.put(addressBytes);
                // and finally put the port number
                buffer.putInt(tcpAddress.getPort());
            }
        } catch (BufferOverflowException e){
            throw new RpcDataSerializationException("Serialization Error : buffer overflow", e);
        }
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        // get request buffer
        ByteBuffer buffer = workRequestProxy.getBuffer();
        try {
            // check the serial version id
            long receivedSerialVersionId = buffer.getLong();
            throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
            // start deserializing
            int addressBytesSize;
            byte[] addressBytes;

            for (int i = 0; i < 2; i++) {
                addressBytesSize = buffer.getInt();
                if (addressBytesSize == -1) { // no address to read
                    continue;
                }
                addressBytes = new byte[addressBytesSize];
                buffer.get(addressBytes);
                try {
                    // create a new InetAddress from the bytes
                    InetAddress ipAddress = InetAddress.getByAddress(addressBytes);
                    // get the port too
                    int port = buffer.getInt();
                    // add the new address
                    if (i == 0) {
                        rdmaAddress = new InetSocketAddress(ipAddress, port);
                    } else {
                        tcpAddress = new InetSocketAddress(ipAddress, port);
                    }
                } catch (UnknownHostException e) {
                    if (i == 0) {
                        throw new RpcDataSerializationException("Cannot deserialize RDMA IP address and port.", e);
                    } else {
                        throw new RpcDataSerializationException("Cannot deserialize TCP IP address and port.", e);
                    }
                }
            }
        }catch (BufferUnderflowException e){
            throw new RpcDataSerializationException("Serialization Error : buffer underflow", e);
        }
    }

    @Override
    public String toString() {
        return "ServerIdentifier{" +
                "rdmaAddress=" + rdmaAddress +
                ", tcpAddress=" + tcpAddress +
                '}';
    }
}
