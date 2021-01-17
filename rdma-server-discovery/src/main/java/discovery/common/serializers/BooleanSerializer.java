package discovery.common.serializers;

import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;

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
        buffer.putLong(serialVersionId);
        if (flag) {
            buffer.put((byte) 1);
        } else {
            buffer.put((byte) 0);
        }
        buffer.flip();
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = workRequestProxy.getBuffer();
        // check the serial version id
        long receivedSerialVersionId = buffer.getLong();
        throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
        byte value = buffer.get();
        flag = (value == 1);
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
