//package com.ignite.streamer;
//
//import org.apache.ignite.cache.affinity.AffinityKeyMapped;
//
//import java.io.Serializable;
//
//public class DpKey implements Serializable {
//    private String key;
//    @AffinityKeyMapped
//    private String devId;
//
//    public DpKey() {
//    }
//
//    public DpKey(String key, String devId) {
//        this.key = key;
//        this.devId = devId;
//    }
//
//    public String getKey() {
//        return key;
//    }
//
//    public void setKey(String key) {
//        this.key = key;
//    }
//
//    public String getDevId() {
//        return devId;
//    }
//
//    public void setDevId(String devId) {
//        this.devId = devId;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        }
//
//        if (o == null || getClass() != o.getClass()) {
//            return false;
//        }
//
//        DpKey key = (DpKey)o;
//
//        return this.key.equals(key.key);
//    }
//
//    @Override
//    public int hashCode() {
//        return key.hashCode();
//    }
//}
