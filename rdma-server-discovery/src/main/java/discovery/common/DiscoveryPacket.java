package discovery.common;

import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import jarg.rdmarpc.rpc.serialization.RpcDataSerializer;

import java.nio.ByteBuffer;


public class DiscoveryPacket extends AbstractRpcPacket {
    private static final long serialVersionId = 1L;

    // Headers ---------------------------------------------------------------------------
    private byte messageType;                       // what kind of message? (e.g. request or a response?)
    private int operationType;                      // what type of operation (rpc function) to invoke?
    private long operationId;                       // unique operation identifier - associate request with response
    private int packetNumber;                       // the number of this packet
                                                        // in case a large message is split in multiple packets

    public DiscoveryPacket(WorkRequestProxy workRequestProxy) {
        super(workRequestProxy);
    }

    @Override
    public void writeToWorkRequestBuffer(RpcDataSerializer rpcDataSerializer) throws RpcDataSerializationException {
        // write headers ------
        ByteBuffer buffer = workRequestProxy.getBuffer();
        buffer.putLong(serialVersionId);
        buffer.put(messageType);
        buffer.putInt(operationType);
        buffer.putLong(operationId);
        buffer.putInt(packetNumber);
        // write payload
        if(rpcDataSerializer != null){
            rpcDataSerializer.writeToWorkRequestBuffer();
        }
    }

    @Override
    public void readHeadersFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = workRequestProxy.getBuffer();
        // read headers -----------------
        long receivedSerialVersionId = buffer.getLong();
        if(receivedSerialVersionId != serialVersionId){
            throw new RpcDataSerializationException("Serial versions do not match. Local version : "+
                    serialVersionId + ", remote version : " + receivedSerialVersionId + ".");
        }
        messageType = buffer.get();
        operationType = buffer.getInt();
        operationId = buffer.getLong();
        packetNumber = buffer.getInt();
    }

    /* *********************************************************
     *   Getters/Setters
     ********************************************************* */

    public static long getSerialVersionId() {
        return serialVersionId;
    }

    public byte getMessageType() {
        return messageType;
    }

    public int getOperationType() {
        return operationType;
    }

    public long getOperationId() {
        return operationId;
    }

    public int getPacketNumber() {
        return packetNumber;
    }

    // Enable method chaining on the setters ---------------

    public DiscoveryPacket setMessageType(byte messageType) {
        this.messageType = messageType;
        return this;
    }

    public DiscoveryPacket setOperationType(int operationType) {
        this.operationType = operationType;
        return this;
    }

    public DiscoveryPacket setOperationId(long operationId) {
        this.operationId = operationId;
        return this;
    }

    public DiscoveryPacket setPacketNumber(int packetNumber) {
        this.packetNumber = packetNumber;
        return this;
    }
}
