package com.circletech.smartconnect.kafkaAPI;

/**
 * Created by Administrator on 2016/12/14.
 */
public class TransportTransducerData implements TransportCommData {

    private Long deviceId;
    private Long code;
    private Long transType;
    private Double transValue;

    private TransportTransducerData() {
    }
    private static class SingletonInstance {
        private static final TransportTransducerData INSTANCE = new TransportTransducerData();
    }

    public static TransportTransducerData getInstance() {
        return SingletonInstance.INSTANCE;
    }


    public Long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Long deviceId) {
        this.deviceId = deviceId;
    }

    public Long getCode() {
        return code;
    }

    public void setCode(Long code) {
        this.code = code;
    }

    public Long getTransType() {
        return transType;
    }

    public void setTransType(Long transType) {
        this.transType = transType;
    }

    public Double getTransValue() {
        return transValue;
    }

    public void setTransValue(Double transValue) {
        this.transValue = transValue;
    }

    @Override
    public String toString() {
        return "com.ceilcell.gps.javaAPI.com.ceilcell.gps.kafkaAPI.TransportTransducerData{" +
                "deviceId=" + deviceId +
                ", code=" + code +
                ", transType=" + transType +
                ", transValue=" + transValue +
                '}';
    }
}
