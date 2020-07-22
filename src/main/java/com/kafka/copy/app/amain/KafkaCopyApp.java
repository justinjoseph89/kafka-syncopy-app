package com.kafka.copy.app.amain;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.exceptions.NotEnoughArgumentException;
import com.kafka.copy.app.exceptions.SourceClusterNotAvailableException;
import com.kafka.copy.app.exceptions.TargetClusterNotAvailableException;
import com.kafka.copy.app.exceptions.TopicNotAvailableException;
import com.kafka.copy.app.kafkautils.KafkaValidator;
import com.kafka.copy.app.kafkautilsimpl.KafkaValidatorImpl;
import com.kafka.copy.app.processors.KafkaCopyAppProcessor;

import static com.kafka.copy.app.constants.KafkaConstants.INPUT_TOPIC_LIST;
import static com.kafka.copy.app.constants.CommonConstants.SPLIT_SEPERATOR;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.TOPIC_NOT_FOUND_EXCEPTION;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.YAML_FILE_NOT_FOUND;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.GRACEFUL_SHUTDOWN;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.GRACEFUL_INT;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import org.apache.log4j.Logger;

public class KafkaCopyApp {
  private static final Logger logger = Logger.getLogger(KafkaCopyApp.class.getName());

  public static <K, V> void main(String[] args) throws NotEnoughArgumentException, TopicNotAvailableException,
      SourceClusterNotAvailableException, TargetClusterNotAvailableException {

    if (args.length < 1) {
      logger.error(YAML_FILE_NOT_FOUND);
      throw new NotEnoughArgumentException(YAML_FILE_NOT_FOUND + args[0]);
    }

    final KafkaConfigReader configReader = new KafkaConfigReader(args[0]);
    final String inputTopicNameList = configReader.getTopicList().get(INPUT_TOPIC_LIST);

    final KafkaValidator validator = new KafkaValidatorImpl<K, V>(inputTopicNameList, configReader);
    validator.isSourceClusterAccessible();
    validator.isTargetClusterAccessible();
    if (!validator.sourceTopicValidator()) {
      logger.error(TOPIC_NOT_FOUND_EXCEPTION);
      throw new TopicNotAvailableException(TOPIC_NOT_FOUND_EXCEPTION);
    }
    validator.targetTopicValidator();
    validator.close();

    final long consumerThreads = configReader.getConsumerThreads();
    CountDownLatch shutdownLatch = new CountDownLatch(Math.toIntExact(consumerThreads));

    Arrays.asList(inputTopicNameList.split(SPLIT_SEPERATOR)).parallelStream().forEach(inputTopic -> {
      LongStream.range(0, consumerThreads).parallel().forEach(threads -> {
        final KafkaCopyAppProcessor<K, V> runner = new KafkaCopyAppProcessor<K, V>(inputTopic, configReader,
            inputTopicNameList);
        runner.start();
      });
    });

    try {
      shutdownLatch.await();
      logger.info(GRACEFUL_SHUTDOWN);
    } catch (InterruptedException e) {
      logger.warn(GRACEFUL_INT);
    }

  }

}
