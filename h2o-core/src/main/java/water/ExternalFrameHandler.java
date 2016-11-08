package water;


import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * This class is used to coordinate the requests for accessing/obtaining h2o frames from non h2o environments
 * ( ie. Spark Executors)
 *
 * The user should use getConnection method to open a connection to h2o node. This method creates the socket channel
 * in a way it is understood by h2o internals.
 *
 * User can write data to h2o frame and read data from h2o frame.
 * When writing, it is expected that empty h2o frame is already in DKV before using the writing API. Also the caller
 * is responsible for finishing the frame once all data has been written to h2o frame. To read more about the writing
 * API, please read documentation of {@link ExternalFrameWriterClient}
 *
 * When reading the data, it is expected that h2o frame is in DKV. To read more about the reading API, please read
 * documentation of {@link ExternalFrameReaderClient}
 *
 *
 */
class ExternalFrameHandler {

    /**
     *  Hints for expected types in order to improve performance network communication.
     *  We make use of these to send and receive data we actually have.
     *
     *  We don't need type for bool since it fits into Byte.
     */
    static final byte EXPECTED_BYTE = 0;
    static final byte EXPECTED_CHAR = 1;
    static final byte EXPECTED_SHORT = 2;
    static final byte EXPECTED_INT = 3;
    static final byte EXPECTED_FLOAT = 4;
    static final byte EXPECTED_LONG = 5;
    static final byte EXPECTED_DOUBLE = 6;
    static final byte EXPECTED_STRING = 7;

    /** Byte signaling that new communication has been started on a existing/newly created socket channel
     *  Since connections can reused at the caller site ( for example spark executor ) we have to identify whether the
     *  the connection has been reused for sending more data or not
     * */
    static final byte INIT_BYTE = 42;

    /**
     * Bytes used for signaling that either reading from h2o frame or writing to h2o frame has finished.
     * It is important for these 2 bytes to be different, otherwise we could confirm writing by reading byte, which
     * would lead to unwanted states.
     */
    static final byte CONFIRM_READING_DONE = 1;
    static final byte CONFIRM_WRITING_DONE = 2;

    // main tasks
    static final byte CREATE_FRAME = 0;
    static final byte DOWNLOAD_FRAME = 1;

    /**
     * Method which receives the {@link SocketChannel} and {@link AutoBuffer} and dispatches the request for further processing
     */
    void process(SocketChannel sock, AutoBuffer ab) throws IOException {
        int requestType = ab.getInt();
        switch (requestType) {
            case CREATE_FRAME:
                ExternalFrameWriterBackend.handleWriteToChunk(sock, ab);
                break;
            case DOWNLOAD_FRAME:
                ExternalFrameReaderBackend.handleReadingFromChunk(sock, ab);
                break;
        }
    }
}
