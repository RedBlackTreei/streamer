package com.pulsar.client;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.shade.com.google.gson.JsonObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author 晓岚[jisen@tuya.com]
 * @date 2019-04-16 16:27
 */
public class PulsarBenchmark {

    private static final String TENANT = "tuya";
    private static final String NAMESPACE = TENANT + "/" + "pulsar-cluster/" + "internal";
    private static final String TOPIC = "persistent://tuya/pulsar-cluster/internal/data-point-topic";

    private static Consumer<DataPoint> consumer;
    private static LongAdder adder = new LongAdder();
    private static Map<Integer, Worker<DataPoint>> workerMap;
    private static Integer workers = 4;

    static {
        ExecutorService executorService = Executors.newFixedThreadPool(workers);
        consumer = TuyaPulsarClient.getInstance().getConsumer(TOPIC , "zeus", SubscriptionType.Failover);
        workerMap = Maps.newHashMap();
        for (int i = 0; i < workers; i++) {
            Worker<DataPoint> worker = new Worker<>(consumer, "" + i);
            workerMap.put(i, worker);
            executorService.submit(worker);
        }
    }

    public static void main(String[] args) {
//        createTopic();
//        int threads = NumberUtils.toInt(args[0]);

        int threads = 1;
//        boolean isProducer = true;
//        if (isProducer) {
//            produce(threads);
//        } else {
//            consume(threads);
//        }
//        produce(threads);
        consume();

    }

    private static void produce(int threads) {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            executorService.submit(new ProducerWorker());
        }

    }

    private static class ProducerWorker implements Runnable {
        private Producer<DataPoint> producer;
        private TuyaPulsarClient client;
        ProducerWorker() {
            client = TuyaPulsarClient.getInstance();
            this.producer = client.getProducer(TOPIC);
        }

        @Override
        public void run() {
            int count = 0;
            while (count < 10000) {
                count ++;
                adder.increment();
                DataPoint dp = new DataPoint();
                String devId = adder.longValue() + "";
                dp.setDevId(devId);
                dp.setDpId(1);
                dp.setValue("value");

                int hashCode = Math.abs(devId.hashCode());
                int partition = hashCode % 64;
                dp.setPartition(partition);

                client.sendMessage(producer, dp);
            }
            System.out.println("关闭生产者");
            producer.closeAsync();
            System.out.println("关闭生产者2");
        }
    }

    private static Worker<DataPoint> getWorker(String key) {
        int hashCode = Math.abs(key.hashCode());
        int index = hashCode % workers;
        return workerMap.get(index);
    }

    private static void consume() {
//        ExecutorService executorService = Executors.newFixedThreadPool(threads);
//        executorService.submit(new ConsumeWorker());
        try {
            while (true) {
                Message<DataPoint> message = consumer.receive();
                String key = message.getKey();
                Worker<DataPoint> worker = getWorker(key);
                worker.addTask(message);
            }
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public static void consume2() {

    }
//
//    private static class ConsumeWorker implements Runnable {
//        private Consumer<DataPoint> consumer;
//        private TuyaPulsarClient client;
//        ConsumeWorker() {
//            client = TuyaPulsarClient.getInstance();
//            this.consumer = client.getConsumer(TOPIC , "zeus", SubscriptionType.Failover);
//        }
//
//        @Override
//        public void run() {
//
//        }
//    }
}
