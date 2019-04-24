package com.pulsar.client;

/**
 * @author 晓岚[jisen@tuya.com]
 * @date 2019-04-16 16:34
 */
public class DataPoint {
    private String devId;
    private Integer dpId;
    private String value;
    private int partition;

    public String getDevId() {
        return devId;
    }

    public void setDevId(String devId) {
        this.devId = devId;
    }

    public Integer getDpId() {
        return dpId;
    }

    public void setDpId(Integer dpId) {
        this.dpId = dpId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "DataPoint{" +
                "devId='" + devId + '\'' +
                ", dpId=" + dpId +
                ", value='" + value + '\'' +
                ", partition=" + partition +
                '}';
    }
}
