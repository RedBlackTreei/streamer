//package com.ignite.streamer;
//
//import com.alibaba.druid.support.json.JSONUtils;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//import com.tuya.ignite.client.domain.DpCache;
//import com.tuya.ignite.client.domain.DpKey;
//import com.tuya.ignite.client.processor.DataPointEntryProcessor;
//import com.tuya.ignite.client.store.DataPointCacheStore;
//import org.apache.commons.lang3.RandomStringUtils;
//import org.apache.commons.lang3.RandomUtils;
//import org.apache.ignite.Ignite;
//import org.apache.ignite.IgniteCache;
//import org.apache.ignite.IgniteDataStreamer;
//import org.apache.ignite.Ignition;
//import org.apache.ignite.binary.BinaryObject;
//import org.apache.ignite.binary.BinaryObjectBuilder;
//import org.apache.ignite.cache.*;
//import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
//import org.apache.ignite.cache.query.FieldsQueryCursor;
//import org.apache.ignite.cache.query.SqlFieldsQuery;
//import org.apache.ignite.config.ConfigurationContext;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.lang.IgniteFuture;
//import org.apache.ignite.lang.IgniteRunnable;
//import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
//import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
//import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
//
//import javax.cache.configuration.FactoryBuilder;
//import javax.cache.processor.EntryProcessorResult;
//import java.util.*;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author 晓岚[jisen@tuya.com]
// * @date 2019-03-01 10:52
// */
//public class App {
//
//    private static final String CACHE_NAME = "data_point_new_2";
//    private static List<String> devIds = new ArrayList<>(3000000);
//
//    public static void main(String[] args) {
//        ConfigurationContext.setEnv("dev");
//        Ignite ignite = getIgnite(true);
////        initCache(ignite);
//        invokeAll(ignite);
//    }
//
//
//    private static Ignite getIgnite(boolean recreateCache) {
//        IgniteConfiguration cfg = new IgniteConfiguration();
//        cfg.setIgniteInstanceName("streamer_test");
//        cfg.setConsistentId("streamer_test");
//        cfg.setPublicThreadPoolSize(16);
//        cfg.setStripedPoolSize(16);
//        cfg.setClientMode(true);
//
////        List<String> addresses = Lists.newArrayList("10.0.216.155:47500..47509", "10.0.230.117:47500..47509", "10.0.251.89:47500..47509");
//        List<String> addresses = Lists.newArrayList("10.0.200.2:47500..47509");
//        TcpDiscoverySpi spi = new TcpDiscoverySpi();
//        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
//        ipFinder.setAddresses(addresses);
//        spi.setIpFinder(ipFinder);
//        cfg.setDiscoverySpi(spi);
//
//        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
//        commSpi.setLocalPort(47175);
//        commSpi.setMessageQueueLimit(2048);
//        cfg.setCommunicationSpi(commSpi);
//
//        Ignite ignite = Ignition.getOrStart(cfg);
//
//        if (recreateCache) {
//            System.out.println("Begin to recreate cache");
//            Collection<String> cacheNames = ignite.cacheNames();
//            for (String cacheName : cacheNames) {
//                if (CACHE_NAME.equals(cacheName)) {
//                    System.out.println("Begin to destroy old cache");
//                    ignite.destroyCache(CACHE_NAME);
//                    System.out.println("Old cache has been destroyed");
//                } else if ("data_point_new".equals(cacheName)) {
//                    System.out.println("Begin to destroy data_point_new");
//                    ignite.destroyCache("data_point_new");
//                    System.out.println("data_point_new has been destroyed");
//                }
//            }
//
//            CacheConfiguration<DpKey, BinaryObject> cacheCfg = getCacheConfiguration(cfg);
//
//            RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
//            affinityFunction.setPartitions(128);
//            affinityFunction.setExcludeNeighbors(true);
//            cacheCfg.setAffinity(affinityFunction);
//
//            ignite.createCache(cacheCfg);
//            System.out.println("Cache has been recreated");
//        }
//
//        return ignite;
//    }
//
//    private static void initCache(Ignite ignite) {
//        System.out.println("Begin to load data, closing WAL");
//        ignite.cluster().disableWal(CACHE_NAME);
//        System.out.println("WAL has been closed");
//
//        IgniteDataStreamer<DpKey, BinaryObject> streamer = ignite.dataStreamer(CACHE_NAME);
//        streamer.skipStore(true);
//        streamer.keepBinary(true);
//        streamer.perNodeBufferSize(10000);
//        streamer.perNodeParallelOperations(32);
//
//        long beginTime = System.currentTimeMillis();
//        int count = 0;
//        Map<DpKey, BinaryObject> map = new HashMap<>(10000);
//        try {
//            while (true) {
//                long t0 = System.currentTimeMillis();
//                String devId = RandomStringUtils.randomAlphanumeric(30);
//                devIds.add(devId);
//                for (int i = 0; i < 5; i++) {
//                    DpCache dp = convertToDpCache(devId, i + 1);
//                    DpKey key = new DpKey(dp.getDpId(), dp.getDevId());
//                    BinaryObject value = ignite.binary().toBinary(dp);
//                    map.put(key, value);
//                }
//                //10000 records per time
//                if (map.size() == 10000) {
//                    streamer.addData(map);
//                    count += map.size();
//                    map.clear();
//                    long t1 = System.currentTimeMillis();
//                    System.out.println("Finished, took = " + (t1 - t0) + "ms, count = " + count);
//                }
//                if (count == 15000000) {
//                    System.out.println("Total time:" + (System.currentTimeMillis() - beginTime) + "ms");
//                    break;
//                }
//            }
//        } finally {
//            ignite.cluster().enableWal(CACHE_NAME);
//            streamer.close();
//            System.out.println("WAL has been enabled");
//        }
//    }
//
//    private static DpCache convertToDpCache(String devId, Integer dpId) {
//        long now = System.currentTimeMillis();
//        DpCache cache = new DpCache();
//        cache.setGmtCreate(now);
//        cache.setGmtModified(now);
//        cache.setDevId(devId);
//        cache.setDpId(dpId);
//        cache.setCode("code");
//        cache.setName("name");
//        cache.setCustomName("customName");
//        cache.setMode("rw");
//        cache.setType("value");
//        cache.setValue("1");
//        cache.setRawValue("11".getBytes());
//        cache.setTime(now);
//        return cache;
//    }
//
//    private static CacheConfiguration<DpKey, BinaryObject> getCacheConfiguration(IgniteConfiguration cfg) {
//        CacheConfiguration<DpKey, BinaryObject> cacheCfg = new CacheConfiguration<>();
//        cacheCfg.setName(CACHE_NAME);
//        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//        cacheCfg.setBackups(1);
//        cacheCfg.setDataRegionName("5G_DataRegion");
//        cacheCfg.setStoreKeepBinary(true);
//        cacheCfg.setQueryParallelism(16);
//
//        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(DataPointCacheStore.class));
//        cacheCfg.setWriteThrough(true);
//        cacheCfg.setWriteBehindEnabled(true);
//        cacheCfg.setWriteBehindFlushThreadCount(2);
//        //每15秒刷新一次
//        cacheCfg.setWriteBehindFlushFrequency(15 * 1000);
//        cacheCfg.setWriteBehindFlushSize(409600);
//        cacheCfg.setWriteBehindBatchSize(1024);
//
//        //2M
//        cacheCfg.setRebalanceBatchSize(2 * 1024 * 1024);
//        cacheCfg.setRebalanceThrottle(100);
//
//        cacheCfg.setSqlIndexMaxInlineSize(64);
//
//        List<QueryEntity> entities = getQueryEntities();
//        cacheCfg.setQueryEntities(entities);
//
//        CacheKeyConfiguration cacheKeyConfiguration = new CacheKeyConfiguration(DpKey.class);
//        cacheCfg.setKeyConfiguration(cacheKeyConfiguration);
//
//        cfg.setCacheConfiguration(cacheCfg);
//        return cacheCfg;
//    }
//
//    private static List<QueryEntity> getQueryEntities() {
//        List<QueryEntity> entities = Lists.newArrayList();
//
//        //配置可见(可被查询)字段
//        QueryEntity entity = new QueryEntity(DpKey.class.getName(), DpCache.class.getName());
//        entity.setTableName("t_data_point_new");
//
//        LinkedHashMap<String, String> map = new LinkedHashMap<>();
//        map.put("id", "java.lang.String");
//        map.put("gmtCreate", "java.lang.Long");
//        map.put("gmtModified", "java.lang.Long");
//        map.put("devId", "java.lang.String");
//        map.put("dpId", "java.lang.Integer");
//        map.put("code", "java.lang.String");
//        map.put("name", "java.lang.String");
//        map.put("customName", "java.lang.String");
//        map.put("mode", "java.lang.String");
//        map.put("type", "java.lang.String");
//        map.put("value", "java.lang.String");
//        map.put("rawValue", byte[].class.getName());
//        map.put("time", "java.lang.Long");
//        map.put("status", "java.lang.Boolean");
//        map.put("uuid", "java.lang.String");
//
//        entity.setFields(map);
//
//        //配置索引信息
//        QueryIndex devIdIdx = new QueryIndex("devId");
//        devIdIdx.setName("idx_devId");
//        devIdIdx.setInlineSize(96);
//        List<QueryIndex> indexes = Lists.newArrayList(devIdIdx);
//        entity.setIndexes(indexes);
//
//        entities.add(entity);
//
//        return entities;
//    }
//
//    private static void benchmark(Ignite ignite, int threads) {
//        for (int i = 0; i < threads; i++) {
//            ExecutorService executorService = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
//                    new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("active-time-limiter-thread-%d").build());
//            executorService.submit(new Task(ignite));
//        }
//    }
//
//    private static class Task implements Runnable {
//        Ignite ignite;
//
//        Task(Ignite ignite) {
//            this.ignite = ignite;
//        }
//
//        @Override
//        public void run() {
//            IgniteCache<DpKey, BinaryObject> cache = ignite.cache(CACHE_NAME);
//
//            //Two query operation and one write operation
//            //why use TimeLimiter? because sometimes write operations will be blocked for some
//            // seconds(may be system thread pool is full), if we do not want the process block
//            //
//            TimeLimiter.withTimeoutOrNull(() -> {
//                int random = RandomUtils.nextInt(0, 3000000);
//                String devId = devIds.get(random);
//                return query(cache, devId);
//            }, 5, TimeUnit.SECONDS, "query");
//            TimeLimiter.withTimeoutOrNull(() -> {
//                int random = RandomUtils.nextInt(0, 3000000);
//                String devId = devIds.get(random);
//                return query(cache, devId);
//            }, 5, TimeUnit.SECONDS, "query");
//
//        }
//    }
//
//    private static List<DpCache> query(IgniteCache<DpKey, BinaryObject> cache, String devId) {
//        String fields = "id, gmtCreate, gmtModified, devId, dpId, code, name, customName, mode, type, value, rawValue, time, status, uuid";
//        String sql = "select " + fields + " from t_data_point_new where devId=?";
//        SqlFieldsQuery query = new SqlFieldsQuery(sql).setArgs(devId);
//
//        FieldsQueryCursor<List<?>> cursor = cache.query(query);
//        List<DpCache> list = Lists.newArrayList();
//        for (List<?> objects : cursor) {
//            DpCache dp = convertToDpCache(objects);
//            list.add(dp);
//        }
//        return list;
//    }
//
//    public static boolean exist(IgniteCache<DpKey, BinaryObject> cache, String devId) {
//        String sql = "select 1 from t_data_point_new where devId=?";
//
//        SqlFieldsQuery query = new SqlFieldsQuery(sql).setArgs(devId);
//        FieldsQueryCursor<List<?>> cursor = cache.query(query);
//        Iterator<List<?>> iterator = cursor.iterator();
//        return iterator.hasNext();
//    }
//
//    public static DpCache queryOne(IgniteCache<DpKey, BinaryObject> cache, String devId, Integer dpId) {
//        String fields = "id, gmtCreate, gmtModified, devId, dpId, code, name, customName, mode, type, value, rawValue, time, status, uuid";
//        String sql = "select " + fields + " from t_data_point_new where devId=? and dpId=?";
//        SqlFieldsQuery query = new SqlFieldsQuery(sql).setArgs(devId, dpId);
//        FieldsQueryCursor<List<?>> cursor = cache.query(query);
//        Iterator<List<?>> iterator = cursor.iterator();
//
//        if (iterator.hasNext()) {
//            return convertToDpCache(iterator.next());
//        } else {
//            return null;
//        }
//    }
//
//
//    private static void initData(Ignite ignite) {
//        IgniteCache<DpKey , BinaryObject> igniteCache = ignite.cache(CACHE_NAME).withKeepBinary();
//        long now = System.currentTimeMillis() / 1000;
//        for (int i = 0; i < 10; i++) {
//            for (int j = 0; j < 5; j++) {
//                DpCache cache = new DpCache();
//                cache.setId(i+"_" +j);
//                cache.setGmtCreate(now);
//                cache.setGmtModified(now);
//                cache.setDevId(i + "x");
//                cache.setDpId(j);
//                cache.setCode("code" + i);
//                cache.setName("name" + i);
//                cache.setCustomName("customName" + i);
//                cache.setMode("rw");
//                cache.setType("value");
//                cache.setValue("value" + i);
//                cache.setRawValue("".getBytes());
//                cache.setTime(now);
//                cache.setStatus(true);
//                cache.setUuid("" + i);
//                igniteCache.putAsync(new DpKey (cache.getDpId(), cache.getDevId()), ignite.binary().toBinary(cache));
//            }
//        }
//    }
//
//    public static void affinityRun(Ignite ignite) {
//        IgniteCache<DpKey , BinaryObject> igniteCache = ignite.cache(CACHE_NAME).withKeepBinary();
//
//        ignite.compute().affinityRunAsync(CACHE_NAME, new DpKey(1, "1"), new IgniteRunnable() {
//            @Override
//            public void run() {
//
//            }
//        });
//    }
//
//    private static void invokeAll(Ignite ignite) {
//        IgniteCache<DpKey , BinaryObject> igniteCache = ignite.cache(CACHE_NAME).withKeepBinary();
//        long now = System.currentTimeMillis() / 1000;
////        initData(ignite);
////
////        DpCache cache = new DpCache();
////        cache.setId(0+"");
////        cache.setGmtModified(now);
////        cache.setDevId(0 + "");
////        cache.setDpId(0);
////        cache.setCode("This is new code" + 0);
////        cache.setName("name00000" + 0);
////        cache.setCustomName("abcd12345");
////        cache.setMode("rw");
////        cache.setType("value");
////        cache.setValue("This is new value111111111");
////        cache.setRawValue("This is new raw value".getBytes());
////        cache.setTime(now);
////
////        try {
////            System.out.println("休息15秒");
////            TimeUnit.SECONDS.sleep(5);
////            System.out.println("休息完毕");
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
////
////        BinaryObject object = ignite.binary().toBinary(cache);
////        DpKey key = new DpKey (cache.getDpId(), cache.getDevId());
////
////        Map<DpKey, DataPointEntryProcessor> map = Maps.newTreeMap();
////        map.put(key, new DataPointEntryProcessor(object));
////
////        IgniteFuture<Map<com.tuya.ignite.client.domain.DpKey , EntryProcessorResult<Object>>> future = igniteCache.invokeAllAsync(map);
////        Map<DpKey, EntryProcessorResult<Object>> resultMap = future.get();
////        for (Map.Entry<DpKey, EntryProcessorResult<Object>> entry : resultMap.entrySet()) {
////            System.out.println("key:" + entry.getKey() + ", value = " + entry.getValue().get());
////        }
////
////        BinaryObject object1 = igniteCache.get(key);
////        System.out.println("value = " + object1);
//
//        boolean exist = exist(igniteCache, "0x");
//        if (exist) {
//            System.out.println("存在111111");
//        } else {
//            System.out.println("不存在111111");
//        }
//
//        boolean exist1 = exist(igniteCache, "2x");
//        if (exist1) {
//            System.out.println("存在1x");
//        } else {
//            System.out.println("不存在1x");
//        }
//
//        DpCache one = queryOne(igniteCache, "1x", 1);
//        if (one != null) {
//            System.out.println("name:" + one.getCustomName() + "|id:" + one.getId());
//        }
//    }
//
//    private BinaryObject convertToBinary(Ignite ignite, DpCache cache) {
//        BinaryObjectBuilder builder = ignite.binary().builder(DpCache.class.getName());
//        builder.setField("gmtModified", cache.getGmtModified());
//        builder.setField("code", cache.getCode());
//        builder.setField("mode", cache.getMode());
//        builder.setField("type", cache.getType());
//        builder.setField("value", cache.getValue());
//        builder.setField("rawValue", cache.getRawValue());
//        builder.setField("status", cache.getStatus());
//        builder.setField("mode", cache.getMode());
//        builder.setField("uuid", cache.getUuid());
//        return builder.build();
//    }
//
//
//    private static DpCache convertToDpCache(List<?> objects) {
//        String id = (String) objects.get(0);
//        Long gmtCreate = (Long) objects.get(1);
//        Long gmtModified = (Long) objects.get(2);
//        String devId = (String) objects.get(3);
//        Integer dpId = (Integer) objects.get(4);
//        String code = (String) objects.get(5);
//        String name = (String) objects.get(6);
//        String customName = (String) objects.get(7);
//        String mode = (String) objects.get(8);
//        String type = (String) objects.get(9);
//        String dpValue = (String) objects.get(10);
//        byte[] rawValue = (byte[]) objects.get(11);
//        Long time = (Long) objects.get(12);
//        Boolean status = (Boolean) objects.get(13);
//        String uuid = (String) objects.get(14);
//
//        DpCache cache = new DpCache();
//        cache.setId(id);
//        cache.setGmtCreate(gmtCreate);
//        cache.setGmtModified(gmtModified);
//        cache.setDevId(devId);
//        cache.setDpId(dpId);
//        cache.setCode(code);
//        cache.setName(name);
//        cache.setCustomName(customName);
//        cache.setMode(mode);
//        cache.setType(type);
//        cache.setValue(dpValue);
//        cache.setRawValue(rawValue);
//        cache.setTime(time);
//        cache.setStatus(status);
//        cache.setUuid(uuid);
//        return cache;
//    }
//}
