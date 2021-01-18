package discovery.common.serializers;

import discovery.common.serializers.BooleanSerializer;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BooleanSerializerTest {

    @Test
    @Tag("Serialization")
    @DisplayName("Serialization Test")
    public void serializationTest(){
        BooleanSerializer serializer = new BooleanSerializer();
        ByteBuffer testBuffer = ByteBuffer.allocate(8+1);
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, null,
                null, testBuffer, null);
        serializer.setWorkRequestProxy(mockProxy);

        boolean flag = true;
        serializer.setFlag(flag);
        assertDoesNotThrow( ()->{ serializer.writeToWorkRequestBuffer(); });
        // do this after writing
        mockProxy.getBuffer().flip();

        long serialVersionId = testBuffer.getLong();
        byte flagByte = testBuffer.get();
        boolean writtenFlag = (flagByte == 1);
        assertDoesNotThrow( ()->{serializer.throwIfSerialVersionInvalid(BooleanSerializer.getSerialVersionId(),
                serialVersionId); });
        assertEquals(flag, writtenFlag);
    }

    @Test
    @Tag("Deserialization")
    @DisplayName("Deserialization Test")
    public void deserializationTest(){
        BooleanSerializer serializer = new BooleanSerializer();
        ByteBuffer testBuffer = ByteBuffer.allocate(8+1);
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, null,
                null, testBuffer, null);
        serializer.setWorkRequestProxy(mockProxy);

        boolean flag = true;
        serializer.setFlag(flag);
        assertDoesNotThrow( ()->{ serializer.writeToWorkRequestBuffer(); });
        // do this after writing
        mockProxy.getBuffer().flip();

            assertDoesNotThrow( ()->{serializer.readFromWorkRequestBuffer();});
        assertEquals(flag, serializer.getFlag());
    }
}
