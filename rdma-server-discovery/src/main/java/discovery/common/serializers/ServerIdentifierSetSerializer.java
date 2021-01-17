package discovery.common.serializers;

import discovery.common.api.ServerIdentifier;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A (de)serializer of a List of {@link InetSocketAddress InetSocketAddresses} that
 * uses a Work Request buffer to write the data to or read the data from.
 */
public class ServerIdentifierSetSerializer extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    private Set<ServerIdentifier> identifiers;

    public ServerIdentifierSetSerializer() {
        super();
    }

    public ServerIdentifierSetSerializer(WorkRequestProxy workRequestProxy){
        super(workRequestProxy);
    }


    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException{
        ByteBuffer buffer = workRequestProxy.getBuffer();
        try {
            // first write the serial version
            buffer.putLong(serialVersionId);
            // then write the list's size
            buffer.putInt(identifiers.size());
            // then for every identifier address, put the ip bytes and port
            for (ServerIdentifier identifier : identifiers) {
                identifier.setWorkRequestProxy(workRequestProxy);
                identifier.writeToWorkRequestBuffer();
            }
        }catch (BufferOverflowException e){
            throw new RpcDataSerializationException("Serialization Error : buffer overflow.", e);
        }
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        // must reset this to avoid errors
        identifiers = new HashSet<>();
        // get request buffer
        ByteBuffer buffer = workRequestProxy.getBuffer();
        // check the serial version id
        long receivedSerialVersionId = buffer.getLong();
        throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
        // read a list of addresses from the buffer
        int listSize = buffer.getInt();
        if(listSize <= 0){
            return;
        }
        int addressBytesSize;
        byte[] addressBytes;
        // then for every identifier address, put the ip bytes and port
        for(int i=0; i<listSize; i++){
            ServerIdentifier identifier = new ServerIdentifier();
            identifier.setWorkRequestProxy(workRequestProxy);
            identifier.readFromWorkRequestBuffer();
            // add identifier to the Set
            identifiers.add(identifier);
        }
    }

    /* *********************************************************
     *   Getters/Setters
     ********************************************************* */

    public Set<ServerIdentifier> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(Set<ServerIdentifier> identifiers) {
        this.identifiers = identifiers;
    }

    @Override
    public WorkRequestProxy getWorkRequestProxy() {
        return this.workRequestProxy;
    }

    @Override
    public void setWorkRequestProxy(WorkRequestProxy workRequestProxy) {
        this.workRequestProxy = workRequestProxy;
    }

    public static long getSerialVersionId() {
        return serialVersionId;
    }
}
