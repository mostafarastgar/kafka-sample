package com.stream.kafka.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.core.env.Environment;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class MessageService {
    @Autowired
    private BinderAwareChannelResolver resolver;

    @Autowired
    private Environment environment;

    @Autowired
    @Output(Source.OUTPUT)
    private MessageChannel messageChannel;

    private static int last = 0;
    private static AtomicInteger partition = new AtomicInteger(0);

    @GetMapping("/message2/{topic}")
    public String message2(@PathVariable String topic) throws UnknownHostException {
        sendMessage2(topic, "message from " + InetAddress.getLocalHost().getHostName());
        return "success";
    }


    private void sendMessage2(final String topicName, final String payload) {
        int nextPartition = getNextPartition();
        messageChannel.send(MessageBuilder
                .withPayload(new POJO() {{
                    setMessage(payload);
                }})
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                .setHeader("partitionKey", nextPartition)
                .setHeader(KafkaHeaders.PARTITION_ID, nextPartition)
                .build());
    }

    @GetMapping("/message/{topic}")
    public String message(@PathVariable String topic) throws UnknownHostException {
        sendMessage(topic, "message from " + InetAddress.getLocalHost().getHostName());
        return "success";
    }

    @Transactional
    public void sendMessage(final String topicName, final String payload) {
        final MessageChannel messageChannel = resolver.resolveDestination(topicName);
        messageChannel.send(new GenericMessage<POJO>(new POJO() {{
            setMessage(payload);
        }}, new HashMap<String, Object>() {{
            put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);
            int nextPartition = getNextPartition();
            put("partitionKey", nextPartition);
            put(KafkaHeaders.PARTITION_ID, nextPartition);
        }}));
    }
//    two-step partitioner
    private int getNextPartition() {
        int i = partition.incrementAndGet();
        if(i-last>1){
            last = i;
        }
        return last % 3;
    }
}

@MessageEndpoint
class MessageListener {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @ServiceActivator(inputChannel = Sink.INPUT)
    public void handleGreetings(@Payload POJO message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header("partitionKey") int softPartition) {
        logger.info("*******This is from kafka {} partition {} soft partition {}*******", message, partition, softPartition);
    }
}
