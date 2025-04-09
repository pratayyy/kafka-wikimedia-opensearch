package com.kwo.wikimedia.connect.task;

import com.kwo.wikimedia.connect.WikimediaConnector;
import com.kwo.wikimedia.connect.processor.EventProcessor;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WikimediaTask extends SourceTask {

    private EventProcessor processor;
    private BlockingQueue<SourceRecord> queue;

    @Override
    public void start(Map<String, String> map) {

        // get all configs and store it in local variables
        String topic = map.get(WikimediaConnector.TOPIC_CONFIG);
        String uri = map.get(WikimediaConnector.URL_CONFIG);
        Long reconnectDuration = Long.parseLong(map.get(WikimediaConnector.RECONNECT_TIME_CONFIG));

        queue = new LinkedBlockingQueue<>();

        // initiate a new processor
        processor = new EventProcessor(uri) {

            @Override
            protected void onClosed() {

            }

            @Override
            protected void onEvent(String event, MessageEvent messageEvent) {
                // each event will be added to stash
                queue.add(new SourceRecord(
                        Collections.singletonMap("source", "wikimedia"),
                        Collections.singletonMap("offset", 0),
                        topic,
                        Schema.STRING_SCHEMA,
                        messageEvent.getData()
                ));
            }
        };

        try {
            queue = new LinkedBlockingQueue<>();
            processor.start(reconnectDuration);
        } catch (Exception e) {
            throw new RuntimeException("unable to start processor", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> records = new LinkedList<>();

        SourceRecord event = queue.poll(1L, TimeUnit.SECONDS);
        if (event == null) {
            return records;
        }

        records.add(event);
        queue.drainTo(records);

        return records;
    }

    @Override
    public void stop() {
        // stop the processor on shutdown
        if (processor != null) {
            try {
                processor.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String version() {
        // will use a static version for now
        return WikimediaConnector.VERSION;
    }
}
