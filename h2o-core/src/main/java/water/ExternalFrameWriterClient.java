package water;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.sql.Timestamp;

import static water.ExternalFrameHandler.*;
import static water.ExternalFrameUtils.writeToChannel;
import static water.ExternalFrameWriterBackend.*;

/**
 * This class is used to create and write data to H2O Frames from non-H2O environments, such as Spark Executors.
 **
 * Example usage of this class:
 *
 * First we need to open the connection to H2O and initialize the writer
 * <pre>{@code
 * byte[] expectedBytes = {ExternalFrameHandler.EXPECTED_BOOL, ExternalFrameHandler.EXPECTED_INT};
 * byte[] vecTypes = {VEC.T_NUM, VEC.T_NUM}
 * ByteChannel channel = ExternalFrameUtils.getConnection("ip:port")
 * ExternalFrameWriter writer = new ExternalFrameWriter(channel);
 * writer.createChunks("frameName", vecTypes, expectedTypes, chunkIdx, numOfRowsToBeWritten)
 * }
 * </pre>
 *
 * Then we can write the data
 * <pre>{@code
 * int rowsWritten = 0;
 * while(rowsWritten < totalNumOfRows){
 *  writer.sendBool(true)
 *  writer.sendInt(657)
 * }
 * </pre>
 *
 * And at the end we need to make sure to force to code wait for all data to be written
 * <pre>{@code
 * writer.waitUntilAllWritten()
 * }
 * </pre>
 */
public class ExternalFrameWriterClient {

    private AutoBuffer ab;
    private ByteChannel channel;
    private byte[] expectedTypes;
    // we discover the current column index based on number of data sent
    private int currentColIdx = 0;

    /**
     * Initialize the External frame writer
     *
     * This method expects expected types in order to ensure we send the data in optimal way.
     * @param channel communication channel to h2o node
     */
    public ExternalFrameWriterClient(ByteChannel channel){
        // using default constructor, AutoBuffer is created with
        // private property _read set to false, in order to satisfy call clearForWriting it has to be set to true
        // which does the call of flipForReading method
        this.ab = new AutoBuffer().flipForReading();
        this.channel = channel;
    }


    /**
     * Create chunks on the h2o backend. This method creates chunk in en empty frame.
     * @param keystr name of the frame
     * @param vecTypes vector types
     * @param expectedTypes expected types
     * @param chunkId chunk index
     * @param totalNumRows total number of rows which is about to be sent
     */
    public void createChunks(String keystr, byte[] vecTypes, byte[] expectedTypes, int chunkId, int totalNumRows) throws IOException {
        ab.clearForWriting(H2O.MAX_PRIORITY);
        ab.put1(ExternalFrameHandler.INIT_BYTE);
        ab.put1(ExternalFrameHandler.CREATE_FRAME);
        ab.putStr(keystr);
        ab.putA1(expectedTypes);
        ab.putA1(vecTypes);
        ab.putInt(totalNumRows);
        ab.putInt(chunkId);
        writeToChannel(ab, channel);
    }

    public void sendBoolean(boolean data) throws IOException{
        ab.put1(data ? 1 : 0);
        putMarkerAndSend(ab, channel, data ? 1 : 0);
    }

    public void sendByte(byte data) throws IOException{
        ab.put1(data);
        putMarkerAndSend(ab, channel, data);
    }

    public void sendChar(char data) throws IOException{
        ab.put2(data);
        putMarkerAndSend(ab, channel, data);
    }

    public void sendShort(short data) throws IOException{
        ab.put2s(data);
        putMarkerAndSend(ab, channel, data);
    }

    public void sendInt(int data) throws IOException{
        ab.putInt(data);
        putMarkerAndSend(ab, channel, data);
    }

    public void putLong(long data) throws IOException{
        ab.put8(data);
        putMarkerAndSend(ab, channel, data);
    }

    public void sendFloat(float data) throws IOException{
        ab.put4f(data);
        increaseCurrentColIdx();
        writeToChannel(ab, channel);
    }

    public void sendDouble(double data) throws IOException{
        ab.put8d(data);
        increaseCurrentColIdx();
        writeToChannel(ab, channel);
    }

    public void sendString(String str) throws IOException{
        ab.putStr(str);
        if(str != null && str.equals(STR_MARKER_NEXT_BYTE_FOLLOWS)){
            ab.put1(MARKER_ORIGINAL_VALUE);
        }
        increaseCurrentColIdx();
        writeToChannel(ab, channel);
    }

    public void putTimestamp(Timestamp timestamp) throws IOException{
        long time = timestamp.getTime();
        ab.put8(time);
        putMarkerAndSend(ab, channel, time);
    }

    public void sendNA() throws IOException{
        switch (expectedTypes[currentColIdx]){
            case EXPECTED_BYTE:
                ab.put1(NUM_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_CHAR:
                ab.put2((char) NUM_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_SHORT:
                ab.put2s(NUM_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_INT:
                ab.putInt(NUM_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_LONG:
                ab.put8(NUM_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_FLOAT:
                ab.put4f(Float.NaN);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_DOUBLE:
                ab.put8d(Double.NaN);
                writeToChannel(ab, channel);
                break;
            case EXPECTED_STRING:
                ab.putStr(STR_MARKER_NEXT_BYTE_FOLLOWS);
                ab.put1(MARKER_NA);
                writeToChannel(ab, channel);
                break;
            default:
                throw new IllegalArgumentException("Unknown expected type " + expectedTypes[currentColIdx]);
        }
    }

    /**
     * This method ensures the application waits for all bytes to be written before continuing in the control flow.
     *
     * It has to be called at the end of writing.
     */
    public void waitUntillAllWriten() throws IOException{
        AutoBuffer confirmAb = new AutoBuffer(channel, null);
        // this needs to be here because confirmAb.getInt() forces this code to wait for result and
        // all the previous work to be done on the recipient side. The assert around it is just additional, not
        // so important check
        assert(confirmAb.getInt() == ExternalFrameHandler.CONFIRM_WRITING_DONE);
    }

    /**
     * Sends another byte as a marker if it's needed and send the data
     */
    private void putMarkerAndSend(AutoBuffer ab, ByteChannel channel, long data) throws IOException{
        if(data == NUM_MARKER_NEXT_BYTE_FOLLOWS){
            // we need to send another byte because zero is represented as 00 ( 2 bytes )
            ab.put1(MARKER_ORIGINAL_VALUE);
        }
        increaseCurrentColIdx();
        writeToChannel(ab, channel);
    }

    private void increaseCurrentColIdx(){
        currentColIdx = (currentColIdx+1) % expectedTypes.length;
    }
}
