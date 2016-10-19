package water;

import water.fvec.Chunk;
import water.fvec.ChunkUtils;
import water.fvec.Frame;
import water.parser.BufferedString;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import static water.ExternalFrameHandler.*;
import static water.ExternalFrameHandler.EXPECTED_STRING;
import static water.ExternalFrameUtils.prepareExpectedTypes;
import static water.ExternalFrameUtils.writeToChannel;

/**
 * This class contains methods used on the h2o backend to read data as from H2O Frame
 */
class ExternalFrameReaderBackend {

    static final byte IS_NA = 1;
    static final byte NOT_NA = 0;

    /**
     * Internal method use on the h2o backend side to handle reading from the chunk from non-h2o environment
     * @param sock socket channel originating from non-h2o node
     * @param initAb {@link AutoBuffer} containing information necessary for preparing backend for reading
     */
    static void handleReadingFromChunk(SocketChannel sock, AutoBuffer initAb) throws IOException {
        String frameKey = initAb.getStr();
        int chunkIdx = initAb.getInt();
        int[] selectedColumnIndices = initAb.getA4();
        assert selectedColumnIndices!=null;
        Frame fr = DKV.getGet(frameKey);
        Chunk[] chunks = ChunkUtils.getChunks(fr, chunkIdx);
        byte[] expectedTypes = prepareExpectedTypes(fr.vecs());

        AutoBuffer ab = new AutoBuffer().flipForReading().clearForWriting(H2O.MAX_PRIORITY);
        ab.putInt(chunks[0]._len); // write num of rows
        writeToChannel(ab, sock);

        // buffer string to be reused for strings to avoid multiple allocation in the loop
        BufferedString valStr = new BufferedString();
        for (int rowIdx = 0; rowIdx < chunks[0]._len; rowIdx++) { // for each row
            for(int cidx: selectedColumnIndices){ // go through the chunks
                ab.flipForReading().clearForWriting(H2O.MAX_PRIORITY); // reuse existing ByteBuffer
                // write flag specifying whether the row is na or not
                if (chunks[cidx].isNA(rowIdx)) {
                    ab.put1(IS_NA);
                } else {
                    ab.put1(NOT_NA);

                    final Chunk chnk = chunks[cidx];
                    switch (expectedTypes[cidx]) {
                        case EXPECTED_BYTE: // this handles both bytes and booleans
                            ab.put1((byte)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_CHAR:
                            ab.put2((char)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_SHORT:
                            ab.put2s((short)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_INT:
                            ab.putInt((int)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_FLOAT:
                            ab.put4f((float)chnk.atd(rowIdx));
                            break;
                        case EXPECTED_LONG:
                            ab.put8(chnk.at8(rowIdx));
                            break;
                        case EXPECTED_DOUBLE:
                            ab.put8d(chnk.atd(rowIdx));
                            break;
                        case EXPECTED_STRING:
                            if (chnk.vec().isCategorical()) {
                                ab.putStr(chnk.vec().domain()[(int) chnk.at8(rowIdx)]);
                            } else if (chnk.vec().isString()) {
                                chnk.atStr(valStr, rowIdx);
                                ab.putStr(valStr.toString());
                            } else if (chnk.vec().isUUID()) {
                                UUID uuid = new UUID(chnk.at16h(rowIdx), chnk.at16l(rowIdx));
                                ab.putStr(uuid.toString());
                            } else {
                                assert false : "Can never be here";
                            }
                            break;
                    }
                }
                writeToChannel(ab, sock);
            }
        }
        ab.flipForReading().clearForWriting(H2O.MAX_PRIORITY);
        ab.put1(ExternalFrameHandler.CONFIRM_READING_DONE);
        writeToChannel(ab, sock);
    }
}
