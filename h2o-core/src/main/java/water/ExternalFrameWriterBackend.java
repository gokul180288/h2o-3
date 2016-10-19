package water;

import water.fvec.ChunkUtils;
import water.fvec.NewChunk;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static water.ExternalFrameHandler.*;
import static water.ExternalFrameHandler.EXPECTED_STRING;
import static water.ExternalFrameUtils.writeToChannel;

/**
 * This class contains methods used on the h2o backend to store incoming data as a H2O Frame
 */
class ExternalFrameWriterBackend {

    // This is used to inform us that another byte is coming.
    // That byte can be either MARKER_ORIGINAL_VALUE or MARKER_NA. If it's MARKER_ORIGINAL_VALUE, that means
    // the value sent is in the previous data sent, otherwise the value is NA.
    static final byte NUM_MARKER_NEXT_BYTE_FOLLOWS = 111;

    // Same as above, but for Strings. Use some unusual character so we limit
    // the chances of sending another byte explaining that
    // the original value is either NA or not
    static final String STR_MARKER_NEXT_BYTE_FOLLOWS = "^";

    // marker informing us that the data are not NA and are stored in the previous byte
    static final byte MARKER_ORIGINAL_VALUE = 0;

    // marker informing us that the data being sent is NA
    static final byte MARKER_NA = 1;

    /**
     * Internal method use on the h2o backend side to handle writing to the chunk from non-h2o environment
     * @param sock socket channel originating from non-h2o node
     * @param ab {@link AutoBuffer} containing information necessary for preparing backend for writing
     */
    static void handleWriteToChunk(SocketChannel sock, AutoBuffer ab) throws IOException {
        String frameKey = ab.getStr();
        byte[] expectedTypes = ab.getA1();
        assert expectedTypes!=null;
        byte[] vecTypes = ab.getA1();
        assert vecTypes!=null;
        int expectedNumRows = ab.getInt();
        int currentRowIdx = 0;
        int currentColIdx = 0;
        int chunk_id = ab.getInt();
        NewChunk[] nchnk = ChunkUtils.createNewChunks(frameKey, vecTypes, chunk_id);
        assert nchnk != null;
        while (currentRowIdx < expectedNumRows) {
            switch (expectedTypes[currentColIdx]) {
                case EXPECTED_BYTE:
                    store(nchnk, ab, currentColIdx, ab.get1());
                    break;
                case EXPECTED_CHAR:
                    store(nchnk, ab, currentColIdx, ab.get2());
                    break;
                case EXPECTED_SHORT:
                    store(nchnk, ab, currentColIdx, ab.get2s());
                    break;
                case EXPECTED_INT:
                    store(nchnk, ab, currentColIdx, ab.getInt());
                    break;
                case EXPECTED_FLOAT:
                    store(nchnk, currentColIdx, ab.get4f());
                    break;
                case EXPECTED_LONG:
                    store(nchnk, ab, currentColIdx, ab.get8());
                    break;
                case EXPECTED_DOUBLE:
                    store(nchnk, currentColIdx, ab.get8d());
                    break;
                case EXPECTED_STRING:
                    store(nchnk, ab, currentColIdx, ab.getStr());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown expected type: " + expectedTypes[currentColIdx]);
            }
            // increase current column index
            currentColIdx = (currentColIdx + 1) % expectedTypes.length;
            currentRowIdx++;
        }
        // close chunks at the end
        ChunkUtils.closeNewChunks(nchnk);

        AutoBuffer outputAb = new AutoBuffer().flipForReading().clearForWriting(H2O.MAX_PRIORITY);
        // flag informing sender that all work is done and
        // chunks are ready to be finalized.
        //
        // This also needs to be sent because in the sender we have to
        // wait for all chunks to be written to DKV; otherwise we get race during finalizing and
        // it happens that we try to finalize frame with chunks not ready yet
        outputAb.putInt(ExternalFrameHandler.CONFIRM_WRITING_DONE);
        writeToChannel(outputAb, sock);
    }

    private static void store(NewChunk[] nchnk, AutoBuffer ab, int currentColIdx, long data){
        if(data == NUM_MARKER_NEXT_BYTE_FOLLOWS){
            // it is either 0 or NA we need to read another byte
            byte marker = ab.get1();
            if(marker == MARKER_NA){
                nchnk[currentColIdx].addNA();
            }else{
                nchnk[currentColIdx].addNum(data);
            }
        }else{
            nchnk[currentColIdx].addNum(data);
        }
    }

    private static void store(NewChunk[] nchnk, int currentColIdx, double data){
        if(Double.isNaN(data)) {
            nchnk[currentColIdx].addNA();
        }else {
            nchnk[currentColIdx].addNum(data);
        }
    }

    private static void store(NewChunk[] nchnk, AutoBuffer ab, int currentColIdx, String data){
        if(data != null && data.equals(STR_MARKER_NEXT_BYTE_FOLLOWS)){
            byte marker = ab.get1();
            if(marker == MARKER_NA){
                nchnk[currentColIdx].addNA();
            }else{
                nchnk[currentColIdx].addStr(data);
            }
        }else{
            nchnk[currentColIdx].addStr(data);
        }
    }

}
