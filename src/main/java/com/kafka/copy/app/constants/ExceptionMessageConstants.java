package com.kafka.copy.app.constants;

/**
 * @author justin
 *
 */
public interface ExceptionMessageConstants {
  static final String TOPIC_NOT_FOUND_EXCEPTION = "Not all input topics or anyone of the input tpic is missing in the source cluster. This is not acceptable. Please recreate and rerun this application";
  static final String YAML_FILE_NOT_FOUND = "yaml file missing from command arguments";
  static final String GRACEFUL_SHUTDOWN = "kafka copy app process has been shutdown gracefully";
  static final String GRACEFUL_INT = "kafka copy app process gracefull shutdown is interrupted";
  static final String SRC_CLUSTER_NA = "source cluster connection could not be established. Please verify the source side kafka broker lists";
  static final String TGT_CLUSTER_NA = "target cluster connection could not be established. Please verify the target side kafka broker lists";
  static final String TOPIC_DESCRIBE_ERROR = "Error while describing the topics and creating topics in the source cluster to target cluster ";

  static final String TGT_NO_TOPICS_FOUND = "No output topics found in the target cluster. Checking manual target properties for creating the topics. if empty or nill then will go for the default cluster level configurations.";
}
