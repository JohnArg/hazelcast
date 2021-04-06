package discovery.common.serializers;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.serialization.AbstractDataSerializer;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * A (de)serializer of booleans that
 * uses a Work Request buffer to write the data to or read the data from.
 */
public class BooleanSerializer extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    private boolean flag;

    public BooleanSerializer() {
        super();
    }

    public BooleanSerializer(WorkRequestProxy workRequestProxy) {
        super(workRequestProxy);
    }

    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException{
        ByteBuffer buffer = workRequestProxy.getBuffer();
        try {
            buffer.putLong(serialVersionId);
            if (flag) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
        }catch (BufferOverflowException e){
            throw new RpcDataSerializationException("Serialization error : buffer overflow.", e);
        }
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = workRequestProxy.getBuffer();
        try {
            // check the serial version id
            long receivedSerialVersionId = buffer.getLong();
            throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
            byte value = buffer.get();
            flag = (value == 1);
        }catch (BufferUnderflowException e){
            throw new RpcDataSerializationException("Serialization error : buffer overflow.", e);
        }
    }

    public boolean getFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public static long getSerialVersionId() {
        return serialVersionId;
    }
}
