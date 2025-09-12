package accord.cluster.debug.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class PseudoUtf8TypeCodec extends TypeCodec<String>
{
    public static final PseudoUtf8TypeCodec TOKEN_CODEC = new PseudoUtf8TypeCodec("org.apache.cassandra.db.marshal.TokenUtf8Type");
    public static final PseudoUtf8TypeCodec TXNID_CODEC = new PseudoUtf8TypeCodec("org.apache.cassandra.db.marshal.TxnIdUtf8Type");
    
    private PseudoUtf8TypeCodec(String type) {
        super(DataType.custom(type), String.class);
    }

    @Override
    public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0) {
            return null;
        }
        byte[] array = new byte[bytes.remaining()];
        bytes.duplicate().get(array);
        return new String(array, StandardCharsets.UTF_8);
    }

    @Override
    public String parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
            return null;
        }
        return value;
    }

    @Override
    public String format(String value) throws InvalidTypeException {
        if (value == null) {
            return "null";
        }
        return "'" + value.replace("'", "''") + "'";
    }
}