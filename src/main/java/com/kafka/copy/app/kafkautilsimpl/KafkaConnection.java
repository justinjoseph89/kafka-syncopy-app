package com.kafka.copy.app.kafkautilsimpl;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.deserializers.KafkaDeserializers;
import com.kafka.copy.app.serializers.KafkaSerializers;

/**
 * @author justin
 *
 */
public class KafkaConnection {

  /**
   * @param consumerID
   * @param configReader
   * @return Properties with kafka details
   */
  public static Properties getSourceConsumerValidationProperties(final KafkaConfigReader configReader) {
    Properties props = getSharedConsumerProperties(configReader);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getSourceServer());
    return props;
  }

  /**
   * @param consumerID
   * @param configReader
   * @return Properties with kafka details
   */
  public static Properties getTargetConsumerValidationProperties(final KafkaConfigReader configReader) {
    Properties props = getSharedConsumerProperties(configReader);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getSourceServer());
    return props;
  }

  /**
   * @param consumerID
   * @param configReader
   * @return Properties with kafka details
   */
  public static Properties getSourceConsumerProperties(final String consumerID, final KafkaConfigReader configReader) {
    Properties props = getSharedConsumerProperties(configReader);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getSourceServer());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerID);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    return props;
  }

  /**
   * @param consumerID
   * @param configReader
   * @return Properties with kafka details
   */
  public static Properties getTargetConsumerProperties(final KafkaConfigReader configReader) {
    Properties props = getSharedConsumerProperties(configReader);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getTargetServer());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerLagFinderGroup");
    return props;
  }

  /**
   * @param configReader
   * @return common kafka Properties
   */
  private static Properties getSharedConsumerProperties(final KafkaConfigReader configReader) {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaDeserializers.getKeyDeserializer(configReader));
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDeserializers.getValueDeserializer(configReader));
    props.put("schema.registry.url", configReader.getSchemaRegistryUrl());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configReader.getAutoOffsetReset());

    return props;

  }

  /**
   * @param configReader
   * @return
   */
  public static Properties getSourceProducerProperties(final KafkaConfigReader configReader) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getSourceServer());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaSerializers.getKeySerializer(configReader));
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSerializers.getValueSerializer(configReader));
    props.put("schema.registry.url", configReader.getSchemaRegistryUrl());
    return props;
  }

}
