package com.smartsoft.wmq.samples;

import com.ibm.mq.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import static com.ibm.mq.constants.MQConstants.*;

/**
 * Created by Igor on 21.05.2014.
 */
public class MQPutTest {

    private static final String QMGR_NAME = "QM01";

    private static final String CHANNEL_NAME = "SVRCON_WSO2";

    private static final String HOSTNAME = "s540";

    private static final int PORT = 1414;


    private static final Logger LOG = Logger.getLogger(MQPutTest.class.getName());
    public static final String APPLICATION_ID = "application_id";

    private MQQueueManager queueManager = null;

    @Before
    public void init() throws MQException {
        final Properties properties = new Properties();
        properties.put(HOST_NAME_PROPERTY, HOSTNAME); // required
        properties.put(PORT_PROPERTY, PORT); // required
        properties.put(CHANNEL_PROPERTY, CHANNEL_NAME); // required
        properties.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES_CLIENT); // opt: if default not defined
        properties.put(USER_ID_PROPERTY, "Igor@localhost"); // opt: if necessary.

        queueManager = new MQQueueManager(QMGR_NAME, properties);
        LOG.info("Connected to " + QMGR_NAME);
    }

    @After
    public void destroy() throws MQException {
        if (queueManager != null && queueManager.isConnected()) {
            queueManager.disconnect();
            LOG.info("QMGR disconnected");
        }
    }

    @Test
    /**
     * MQPUT out of syncpoint
     */
    public void put() throws MQException, IOException {
        MQQueue queue = queueManager.accessQueue("Q1", MQOO_OUTPUT);

        MQMessage message = new MQMessage();

        message.writeString("UNIXTime: " + System.currentTimeMillis());

        message.setIntProperty("application_id", 11622717);

        MQPutMessageOptions putSpec = new MQPutMessageOptions();
        putSpec.options =  putSpec.options | MQPMO_NEW_MSG_ID | MQPMO_NO_SYNCPOINT;

        queue.put(message, putSpec);
        LOG.info("Message PUT with messageId = " + UUID.nameUUIDFromBytes(message.messageId) + " application_id = " + message.getIntProperty(APPLICATION_ID));

    }
}
