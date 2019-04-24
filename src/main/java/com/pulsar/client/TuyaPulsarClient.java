package com.pulsar.client;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.pulsar.client.api.*;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author 晓岚[jisen@tuya.com]
 * @date 2019-04-16 20:07
 */
public class TuyaPulsarClient {
    private LongAdder totalProduced;
    private LongAdder produceGreaterThan5;
    private LongAdder produceGreaterThan10;
    private LongAdder produceGreaterThan20;
    private LongAdder produceGreaterThan50;
    private LongAdder produceGreaterThan100;

    private LongAdder totalConsumed;
    private LongAdder consumeGreaterThan5;
    private LongAdder consumeGreaterThan10;
    private LongAdder consumeGreaterThan20;
    private LongAdder consumeGreaterThan50;
    private LongAdder consumeGreaterThan100;
    private LongAdder consumeGreaterThan200;

    private static TuyaPulsarClient client;

    private PulsarClient pulsarClient;

    private TuyaPulsarClient() {
        totalProduced = new LongAdder();
        produceGreaterThan5 = new LongAdder();
        produceGreaterThan10 = new LongAdder();
        produceGreaterThan20 = new LongAdder();
        produceGreaterThan50 = new LongAdder();
        produceGreaterThan100 = new LongAdder();

        totalConsumed = new LongAdder();
        consumeGreaterThan5 = new LongAdder();
        consumeGreaterThan10 = new LongAdder();
        consumeGreaterThan20 = new LongAdder();
        consumeGreaterThan50 = new LongAdder();
        consumeGreaterThan100 = new LongAdder();
        consumeGreaterThan200 = new LongAdder();

        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl("pulsar://10.0.200.87:6650")
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public static TuyaPulsarClient getInstance() {
        if (client != null) {
            return client;
        }
        synchronized (TuyaPulsarClient.class) {
            if (client == null) {
                client = new TuyaPulsarClient();
            }
            return client;
        }
    }

    public Consumer<DataPoint> getConsumer(String topic, String subscriptionName, SubscriptionType type) {
        try {
            return pulsarClient.newConsumer(Schema.JSON(DataPoint.class))
                    .topic(topic)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(type)
                    .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(3).build())
                    .ackTimeout(2, TimeUnit.SECONDS)
//                    .messageListener((MessageListener<DataPoint>) (consumer, msg) -> {
//                        DataPoint dataPoint = msg.getValue();
//                        System.out.println(Thread.currentThread().getName() + ",消费成功, devId = " + dataPoint.getDevId() + "|" + dataPoint.getPartition() + "|topic=" + msg.getTopicName());
//                        consumer.acknowledgeAsync(msg);
//                    })
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }


    private void doConsume(int workers) {
        List<BlockingQueue<DataPoint>> workerQueues = Lists.newArrayList();
        for (int i = 0; i < workers; i++) {
            workerQueues.add(new ArrayBlockingQueue<>(50));
        }

    }


    public Producer<DataPoint> getProducer(String topic) {
        try {
            return pulsarClient.newProducer(Schema.JSON(DataPoint.class)).topic(topic)
                    .sendTimeout(2, TimeUnit.SECONDS)
                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                    .messageRouter(new MessageRouter() {
                        @Override
                        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                            DataPoint dp = (DataPoint)msg.getValue();
                            String key = dp.getDevId();
//                            System.out.println("key = " + key);
                            int hashCode = Math.abs(key.hashCode());
                            int partitions = metadata.numPartitions();
                            int partition = hashCode % partitions;
                            dp.setPartition(partition);
                            System.out.println("message = " + dp);
                            return partition;
                        }
                    })
                    .create();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void sendMessage(Producer<DataPoint> producer, DataPoint message) {
        totalProduced.increment();

        StopWatch watch = StopWatch.createStarted();
        try {
            producer.newMessage()
                    .key(message.getDevId())
                    .value(message)
                    .send();
//            System.out.println("消息发送成功,devId=" + message.getDevId());

            watch.split();
            long took = watch.getSplitTime();
            if (took > 5) {
                produceGreaterThan5.increment();
                if (took > 10) {
                    produceGreaterThan10.increment();
                    if (took > 20) {
                        produceGreaterThan20.increment();
                        if (took > 50) {
                            produceGreaterThan50.increment();
                            if (took > 100) {
                                produceGreaterThan100.increment();
                            }
                        }
                    }
                }
            }


        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

//    public DataPoint consumeMessage(Consumer<DataPoint> consumer) {
//        DataPoint dataPoint = null;
//        try {
//            Message<DataPoint> message = consumer.receive();
//            dataPoint = message.getValue();
//            System.out.println(Thread.currentThread().getName() + ",消费成功, devId = " + dataPoint.getDevId() + "|" + dataPoint.getPartition());
//            consumer.acknowledge(message);
//        } catch (PulsarClientException e) {
//            consumer.redeliverUnacknowledgedMessages();
//            e.printStackTrace();
//        }
//        return dataPoint;
//    }

    public void consumeMessage(Consumer<DataPoint> consumer) {
        while (true) {
            try {
                Message<DataPoint> message = consumer.receive();

            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }
    }

    public void test(Consumer<DataPoint> consumer) {
    }


}
