package com.pulsar.client;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author 晓岚[jisen@tuya.com]
 * @date 2019-04-23 19:02
 */
public class Worker<T> implements Runnable {

    private Consumer<T> consumer;
    private BlockingQueue<Message<T>> queue;
    private String workerName;

    public Worker(Consumer<T> consumer, String workerName) {
        this.consumer = consumer;
        this.queue = new ArrayBlockingQueue<>(50);
        this.workerName = workerName;
    }

    @Override
    public void run() {
        while (true) {
            Message<T> message;
            try {
                message = queue.take();
                System.out.println(workerName + "|key=" + message.getKey() + "|topic=" + message.getTopicName());
                consumer.acknowledge(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
                consumer.redeliverUnacknowledgedMessages();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }
    }

    public void addTask(Message<T> message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int hashCode() {
        return workerName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return String.valueOf(obj).equals(workerName);
    }
}
