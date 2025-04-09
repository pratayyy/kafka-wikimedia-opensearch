package com.kwo.wikimedia.connect.processor;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Abstract event processor
 */
public abstract class EventProcessor {

    EventProcessor _eventProcessor = this;

    CompletableFuture<Long> promise;

    private Long count = 0L;

    private final String uri;

    private BackgroundEventSource eventSource;

    public EventProcessor(String uri) {
        this.uri = uri;
    }

    /**
     * Override this method to do on close callback
     */
    protected abstract void onClosed();

    /**
     * Override this method to handle each message
     *
     * @param event
     * @param messageEvent
     */
    protected abstract void onEvent(String event, MessageEvent messageEvent);

    /**
     * shuts down the processor
     *
     * @throws Exception throw if the processor hasn't started
     */
    public void shutdown() throws Exception {
        if (eventSource == null) {
            promise.complete(count);
            throw new Exception("trying to stop EventProcessor without starting");
        } else {
            eventSource.close();
        }
    }

    /**
     * start the processor
     *
     * @param reconnectTime reconnect if processor in case of error for this duration
     * @return a promise which the caller can wait on ; this promise will complete once the processor is shutdown
     * @throws Exception throws if already started
     */
    public CompletableFuture<Long> start(Long reconnectTime) throws Exception {
        if (eventSource != null) {
            throw new Exception("trying to start already started EventProcessor");
        }
        if (reconnectTime == null) {
            reconnectTime = 3000L;
        }

        EventSource.Builder builder = new EventSource.Builder(URI.create(uri)).retryDelay(reconnectTime, TimeUnit.MILLISECONDS);

        BackgroundEventHandler eventHandler = new CustomEventHandler();
        BackgroundEventSource.Builder backBuilder = new BackgroundEventSource.Builder(eventHandler, builder);
        eventSource = backBuilder.build();

        promise = new CompletableFuture<>();

        eventSource.start();

        return promise;
    }

    /**
     * a custom event handler that delegates to processor methods
     */
    public class CustomEventHandler implements BackgroundEventHandler {

        @Override
        public void onOpen() {

        }

        @Override
        public void onClosed() {
            _eventProcessor.onClosed();
            promise.complete(count);
        }

        @Override
        public void onMessage(String event, MessageEvent messageEvent) {
            count++;
            onEvent(event, messageEvent);
        }

        @Override
        public void onComment(String s) {

        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
        }
    }

}
