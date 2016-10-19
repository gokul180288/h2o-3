package water.fvec;

import org.junit.BeforeClass;
import org.junit.Test;
import water.ExternalFrameHandler;
import water.ExternalFrameReader;
import water.H2O;
import water.TestUtil;

import java.io.IOException;
import java.nio.channels.ByteChannel;

public class ExternalFrameReaderTest extends TestUtil{
    @BeforeClass()
    public static void setup() {
        stall_till_cloudsize(2);
    }

    // We keep this one for assertion errors occurred in different threads
    private volatile AssertionError exc;

    @Test
    public void testReading() throws IOException, InterruptedException {

        final String frameName = "testFrame";
        final long[] chunkLayout = {2, 2, 2, 1};
        final Frame testFrame = new TestFrameBuilder()
                .withName(frameName)
                .withColNames("ColA", "ColB")
                .withVecTypes(Vec.T_NUM, Vec.T_STR)
                .withDataForCol(0, ard(Double.NaN, 1, 2, 3, 4, 5.6, 7))
                .withDataForCol(1, ar("A", "B", "C", "E", "F", "I", "J"))
                .withChunkLayout(chunkLayout)
                .build();


        // create frame
        final String[] nodes = new String[H2O.CLOUD._memary.length];
        // get ip and ports of h2o nodes
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = H2O.CLOUD._memary[i].getIpPortString();
        }

        final int nChunks = testFrame.anyVec().nChunks();

        // we will read from all chunks at the same time
        Thread[] threads = new Thread[nChunks];

        try {
            // open all connections in connStrings array
            for (int idx = 0; idx < nChunks; idx++) {
                final int currentChunkIdx = idx;
                threads[idx] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            ByteChannel sock = ExternalFrameHandler.getConnection(nodes[currentChunkIdx % 2]);
                            ExternalFrameReader reader = new ExternalFrameReader(sock, frameName, currentChunkIdx, new int[]{0, 1});

                            int rowsRead = 0;
                            assert reader.getNumRows() == chunkLayout[currentChunkIdx];

                            while (rowsRead < reader.getNumRows()) {

                                if (rowsRead == 0 & currentChunkIdx == 0) {
                                    boolean isNA = reader.readIsNA();
                                    assert isNA : "[0,0] in chunk 0 should be NA";
                                } else {
                                    if (!reader.readIsNA()) {
                                        reader.readDouble();
                                    }
                                }

                                if (!reader.readIsNA()) {
                                    reader.readString();
                                }
                                rowsRead++;
                            }

                            assert rowsRead == reader.getNumRows() : "Num or rows read was " + rowsRead + ", expecting " + reader.getNumRows();

                            reader.waitUntilAllReceived();
                            sock.close();
                        } catch (AssertionError e) {
                            exc = e;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
                threads[idx].start();
            }

            // wait for all writer thread to finish
            for (Thread t : threads) {
                t.join();
                if (exc != null) {
                    throw exc;
                }
            }
        }finally {
            testFrame.remove();
        }
    }

}
