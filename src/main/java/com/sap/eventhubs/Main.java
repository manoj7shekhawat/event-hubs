package com.sap.eventhubs;

/**
 * @author manoj7shekhawat
 * Date: 14/12/2022
 */

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.IterableStream;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.qpid.proton.engine.Sender;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.time.Duration;

/**
 * Main class for event-hub testing with SPN
 */
public class Main {
    private static final String TENANT_ID = System.getProperty("tenant_id");
    private static final String CLIENT_ID = System.getProperty("client_id");
    private static final String CLIENT_SECRET = System.getProperty("client_secret");
    private static final String EH_FQDN = System.getProperty("eh_fdqn");
    private static final String EH_NAME = System.getProperty("eh_name");
    private static final String CONSUMER_GROUP = System.getProperty("consumer_group");
    private static final int MESSAGES_TO_CONSUME = 2;
    private static final int DURATION_TO_WAIT = 40;
    private static final String SAMPLE_FILE_NAME = "sample_message.json";

    /**
     * main method
     * @param args
     */
    public static void main(String[] args) throws FileNotFoundException {

        if (args.length != 1) {
            System.out.println("Please pass required args: c/p");
        } else if (args[0].equalsIgnoreCase("c")) {
            System.out.println("Running consumer...");

            TokenCredential credential = getTokenCredential();
            EventHubConsumerClient consumer = createEventHubConsumerClient(credential);
            printMessages(consumer);
            consumer.close();

        } else if (args[0].equalsIgnoreCase("p")) {
            System.out.println("Running producer...");

            TokenCredential credential = getTokenCredential();
            EventHubProducerClient producer = createEventHubProducerClient(credential);
            sendMessages(producer);
            producer.close();

        } else {
            System.out.println("Invalid arguments. Valid values are: c, p");
        }

    }

    /**
     * Create TokenCredential object for SPN
     * @return TokenCredential
     */
    private static TokenCredential getTokenCredential() {
        return new ClientSecretCredentialBuilder()
                .tenantId(TENANT_ID)
                .clientId(CLIENT_ID)
                .clientSecret(CLIENT_SECRET)
                .build();
    }

    /**
     * create EventHubConsumerClient
     * @param credential - TokenCredential
     * @return EventHubConsumerClient
     */
    private static EventHubConsumerClient createEventHubConsumerClient(TokenCredential credential) {
        return new EventHubClientBuilder()
                .credential(EH_FQDN, EH_NAME, credential)
                .consumerGroup(CONSUMER_GROUP)
                .buildConsumerClient();
    }

    private static EventHubProducerClient createEventHubProducerClient(TokenCredential credential) {
        return new EventHubClientBuilder()
                .credential(EH_FQDN, EH_NAME, credential)
                .consumerGroup(CONSUMER_GROUP)
                .buildProducerClient();
    }

    private static void printMessages(EventHubConsumerClient consumer) {
        IterableStream<String> partitionIds = consumer.getPartitionIds();
        for (String partition: partitionIds) {
            System.out.println("Partition: " + partition);
            IterableStream<PartitionEvent> events = consumer.receiveFromPartition(
                    partition,
                    MESSAGES_TO_CONSUME,
                    EventPosition.earliest(),
                    Duration.ofSeconds(DURATION_TO_WAIT)
            );
            for (PartitionEvent event : events) {
                System.out.println("Event: " + event.getData().getBodyAsString());
            }
        }
    }

    private static void sendMessages(EventHubProducerClient producer) throws FileNotFoundException {
        CreateBatchOptions options = new CreateBatchOptions().setPartitionId("0");

        EventDataBatch batch = producer.createBatch(options);
        batch.tryAdd(
                new EventData(
                        new Gson()
                                .fromJson(
                                        new FileReader(Sender.class.getClassLoader().getResource(SAMPLE_FILE_NAME).getFile()),
                                        JsonObject.class)
                                .toString()
                                .getBytes(Charset.defaultCharset())
                )
        );
        producer.send(batch);
        System.out.println("Message sent successfully to EH.");
    }
}
