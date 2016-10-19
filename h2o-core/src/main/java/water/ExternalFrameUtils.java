package water;

import water.fvec.Vec;

import java.io.IOException;
import java.nio.channels.ByteChannel;

import static water.ExternalFrameHandler.*;

/**
 * Various utilities methods to help with external frame handling
 */
public class ExternalFrameUtils {

    /**
     * Get connection to a specific h2o node. The caller of this method is usually non-h2o node who wants to read h2o
     * frames or write to h2o frames from non-h2o environment, such as Spark executor.
     */
    public static ByteChannel getConnection(String h2oNodeHostname, int h2oNodeApiPort) throws IOException{
        return H2ONode.openChan(TCPReceiverThread.TCP_EXTERNAL, null, h2oNodeHostname, h2oNodeApiPort +1);

    }

    public static ByteChannel getConnection(String ipPort) throws IOException{
        String[] split = ipPort.split(":");
        return getConnection(split[0], Integer.parseInt(split[1]));
    }

    static void writeToChannel(AutoBuffer ab, ByteChannel channel) throws IOException {
        ab.flipForReading();
        while (ab._bb.hasRemaining()){
            channel.write(ab._bb);
        }
    }

    /**
     * Used to detect the minimal numeric type to which all vector's values fit
     */
    static private byte detectNumericType(Vec v){
        double min = v.min();
        double max = v.max();
        if (v.isInt()) {
            if (min > Byte.MIN_VALUE && max <Byte.MAX_VALUE) {
                return EXPECTED_BYTE;
            } else if (min > Short.MIN_VALUE && max < Short.MAX_VALUE) {
                return EXPECTED_SHORT;
            } else if (min > Character.MIN_VALUE && max < Character.MAX_VALUE) {
                return EXPECTED_CHAR;
            }
            else if (min > Integer.MIN_VALUE && max < Integer.MAX_VALUE) {
                return EXPECTED_INT;
            } else {
                return EXPECTED_LONG;
            }
        }else{
            if(min > Float.MIN_VALUE && max < Float.MAX_VALUE){
                return EXPECTED_FLOAT;
            }else{
                return EXPECTED_DOUBLE;
            }
        }
    }

    static byte[] prepareExpectedTypes(Vec[] vecs) {
        byte[] expectedTypes = new byte[vecs.length];
        for (int i = 0; i < vecs.length; i++) {
            switch (vecs[i].get_type()) {
                case Vec.T_BAD:
                    expectedTypes[i] = EXPECTED_BYTE;
                    break;
                case Vec.T_NUM:
                    expectedTypes[i] = detectNumericType(vecs[i]);
                    break;
                case Vec.T_CAT:
                    expectedTypes[i] = EXPECTED_STRING;
                    break;
                case Vec.T_UUID:
                    expectedTypes[i] = EXPECTED_STRING;
                    break;
                case Vec.T_STR:
                    expectedTypes[i] = EXPECTED_STRING;
                    break;
                case Vec.T_TIME:
                    expectedTypes[i] = EXPECTED_LONG;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown vector type " + vecs[i].get_type());
            }
        }
        return expectedTypes;
    }
}
