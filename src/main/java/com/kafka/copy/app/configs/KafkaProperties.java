package com.kafka.copy.app.configs;

import java.util.HashMap;
import java.util.Map;

/**
 * @author justin
 *
 */
public class KafkaProperties {

//	private String appId = "DEFAULT_APP_ID";
  private String bootstrapServers = "localhost:9092";
  private String bootstrapServersTarget = "localhost:9093";
  private String zookeeperHost = "localhost";
  private String defaultKeySerde = "String";
  private String defaultValueSerde = "String";
  private String appVersion = "0";
  private boolean zkNodeUpd = false;
  private float appDeltaValue = 100L;
  private float appSmallDeltaValue = 1L;
  private long appSleepTimeMs = 100;
  private String schemaRegistyUrl = "http://localhost:8081/";
  private String autoOffsetReset = "earliest";
  private long numConsumerThreads = 1L;
  private Map<String, String> topics = new HashMap<>();
  private Map<String, String> topicsFields = new HashMap<>();

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getBootstrapServersTarget() {
    return bootstrapServersTarget;
  }

  void setBootstrapServersTarget(String bootstrapServersTarget) {
    this.bootstrapServersTarget = bootstrapServersTarget;
  }

  public String getZookeeperHost() {
    return zookeeperHost;
  }

  void setZookeeperHost(String zookeeperHost) {
    this.zookeeperHost = zookeeperHost;
  }

  public String getDefaultKeySerde() {
    return defaultKeySerde;
  }

  void setDefaultKeySerde(String defaultKeySerde) {
    this.defaultKeySerde = defaultKeySerde;
  }

  public String getDefaultValueSerde() {
    return defaultValueSerde;
  }

  void setDefaultValueSerde(String defaultValueSerde) {
    this.defaultValueSerde = defaultValueSerde;
  }

  public String getAppVersion() {
    return appVersion;
  }

  void setAppVersion(String appVersion) {
    this.appVersion = appVersion;
  }

  public boolean isZkNodeUpd() {
    return zkNodeUpd;
  }

  void setZkNodeUpd(boolean zkNodeUpd) {
    this.zkNodeUpd = zkNodeUpd;
  }

  public float getAppDeltaValue() {
    return appDeltaValue;
  }

  void setAppDeltaValue(float appDeltaValue) {
    this.appDeltaValue = appDeltaValue;
  }

  public float getAppSmallDeltaValue() {
    return appSmallDeltaValue;
  }

  void setAppSmallDeltaValue(float appSmallDeltaValue) {
    this.appSmallDeltaValue = appSmallDeltaValue;
  }

  public long getAppSleepTimeMs() {
    return appSleepTimeMs;
  }

  void setAppSleepTimeMs(long appSleepTimeMs) {
    this.appSleepTimeMs = appSleepTimeMs;
  }

  public String getSchemaRegistyUrl() {
    return schemaRegistyUrl;
  }

  void setSchemaRegistyUrl(String schemaRegistyUrl) {
    this.schemaRegistyUrl = schemaRegistyUrl;
  }

  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  void setAutoOffsetReset(String autoOffsetReset) {
    this.autoOffsetReset = autoOffsetReset;
  }

  public long getNumConsumerThreads() {
    return numConsumerThreads;
  }

  void setNumConsumerThreads(long numConsumerThreads) {
    this.numConsumerThreads = numConsumerThreads;
  }

  public Map<String, String> getTopics() {
    return topics;
  }

  void setTopics(Map<String, String> topics) {
    this.topics = topics;
  }

  public Map<String, String> getTopicsFields() {
    return topicsFields;
  }

  void setTopicsFields(Map<String, String> topicsFields) {
    this.topicsFields = topicsFields;
  }

}
