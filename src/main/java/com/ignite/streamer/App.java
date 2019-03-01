package com.ignite.streamer;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import java.util.*;

/**
 * @author 晓岚[jisen@tuya.com]
 * @date 2019-03-01 10:52
 */
public class App {

    private static final String CACHE_NAME = "data_point_new_2";

    public static void main(String[] args) {
        Ignite ignite = getIgnite(true);
        initCache(ignite);
    }

    private static Ignite getIgnite(boolean recreateCache) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("streamer_test");
        cfg.setConsistentId("streamer_test");
        cfg.setPublicThreadPoolSize(16);
        cfg.setStripedPoolSize(16);
        cfg.setClientMode(true);

        List<String> addresses = Lists.newArrayList("10.0.216.155:47500..47509", "10.0.230.117:47500..47509", "10.0.251.89:47500..47509");
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(addresses);
        spi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(spi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setLocalPort(47175);
        commSpi.setMessageQueueLimit(2048);
        cfg.setCommunicationSpi(commSpi);

        Ignite ignite = Ignition.getOrStart(cfg);

        if (recreateCache) {
            System.out.println("Begin to recreate cache");
            Collection<String> cacheNames = ignite.cacheNames();
            for (String cacheName : cacheNames) {
                if (CACHE_NAME.equals(cacheName)) {
                    System.out.println("Begin to destroy old cache");
                    ignite.destroyCache(CACHE_NAME);
                    System.out.println("Old cache has been destroyed");
                } else if ("data_point_new".equals(cacheName)) {
                    System.out.println("Begin to destroy data_point_new");
                    ignite.destroyCache("data_point_new");
                    System.out.println("data_point_new has been destroyed");
                }
            }

            CacheConfiguration<DpKey, BinaryObject> cacheCfg = getCacheConfiguration(cfg);

            RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
            affinityFunction.setPartitions(128);
            affinityFunction.setExcludeNeighbors(true);
            cacheCfg.setAffinity(affinityFunction);

            ignite.createCache(cacheCfg);
            System.out.println("Cache has been recreated");
        }

        return ignite;
    }

    private static void initCache(Ignite ignite) {
        System.out.println("Begin to load data, closing WAL");
        ignite.cluster().disableWal(CACHE_NAME);
        System.out.println("WAL has been closed");

        IgniteDataStreamer<DpKey, BinaryObject> streamer = ignite.dataStreamer(CACHE_NAME);
        streamer.skipStore(true);
        streamer.keepBinary(true);
        streamer.perNodeBufferSize(10000);
        streamer.perNodeParallelOperations(32);

        long beginTime = System.currentTimeMillis();
        int count = 0;
        Map<DpKey, BinaryObject> map = new HashMap<>(10000);
        try {
            while (true) {
                long t0 = System.currentTimeMillis();
                String devId = RandomStringUtils.randomAlphanumeric(30);
                for (int i = 0; i < 5; i++) {
                    DpCache dp = convertToDpCache(devId, i + 1);
                    DpKey key = new DpKey(dp.getDevId() + "_" + dp.getDpId(), dp.getDevId());
                    BinaryObject value = ignite.binary().toBinary(dp);
                    map.put(key, value);
                }
                //10000 records per time
                if (map.size() == 10000) {
                    streamer.addData(map);
                    count += map.size();
                    map.clear();
                    long t1 = System.currentTimeMillis();
                    System.out.println("Finished, took = " + (t1 - t0) + "ms, count = " + count);
                }
                if (count == 40000000) {
                    System.out.println("Total time:" + (System.currentTimeMillis() - beginTime) + "ms");
                    break;
                }
            }
        } finally {
            ignite.cluster().enableWal(CACHE_NAME);
            streamer.close();
            System.out.println("WAL has been enabled");
        }
    }

    private static DpCache convertToDpCache(String devId, Integer dpId) {
        long now = System.currentTimeMillis();
        DpCache cache = new DpCache();
        cache.setGmtCreate(now);
        cache.setGmtModified(now);
        cache.setDevId(devId);
        cache.setDpId(dpId);
        cache.setCode("code");
        cache.setName("name");
        cache.setCustomName("customName");
        cache.setMode("rw");
        cache.setType("value");
        cache.setValue("1");
        cache.setRawValue("11".getBytes());
        cache.setTime(now);
        return cache;
    }

    private static CacheConfiguration<DpKey, BinaryObject> getCacheConfiguration(IgniteConfiguration cfg) {
        CacheConfiguration<DpKey, BinaryObject> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setBackups(1);
        cacheCfg.setDataRegionName("5G_DataRegion");
        cacheCfg.setStoreKeepBinary(true);
        cacheCfg.setQueryParallelism(16);

        //2M
        cacheCfg.setRebalanceBatchSize(2 * 1024 * 1024);
        cacheCfg.setRebalanceThrottle(100);

        cacheCfg.setSqlIndexMaxInlineSize(128);

        List<QueryEntity> entities = getQueryEntities();
        cacheCfg.setQueryEntities(entities);

        CacheKeyConfiguration cacheKeyConfiguration = new CacheKeyConfiguration(DpKey.class);
        cacheCfg.setKeyConfiguration(cacheKeyConfiguration);

        cfg.setCacheConfiguration(cacheCfg);
        return cacheCfg;
    }

    private static List<QueryEntity> getQueryEntities() {
        List<QueryEntity> entities = Lists.newArrayList();

        //配置可见(可被查询)字段
        QueryEntity entity = new QueryEntity(DpKey.class.getName(), DpCache.class.getName());
        entity.setTableName("t_data_point_new");

        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("id", "java.lang.String");
        map.put("gmtCreate", "java.lang.Long");
        map.put("gmtModified", "java.lang.Long");
        map.put("devId", "java.lang.String");
        map.put("dpId", "java.lang.Integer");
        map.put("code", "java.lang.String");
        map.put("name", "java.lang.String");
        map.put("customName", "java.lang.String");
        map.put("mode", "java.lang.String");
        map.put("type", "java.lang.String");
        map.put("value", "java.lang.String");
        map.put("rawValue", byte[].class.getName());
        map.put("time", "java.lang.Long");
        map.put("status", "java.lang.Boolean");
        map.put("uuid", "java.lang.String");

        entity.setFields(map);

        //配置索引信息
        QueryIndex devIdIdx = new QueryIndex("devId");
        devIdIdx.setName("idx_devId");
        devIdIdx.setInlineSize(32);
        List<QueryIndex> indexes = Lists.newArrayList(devIdIdx);
        entity.setIndexes(indexes);

        entities.add(entity);

        return entities;
    }
}
