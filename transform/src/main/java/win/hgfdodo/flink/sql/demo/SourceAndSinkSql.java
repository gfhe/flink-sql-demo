package win.hgfdodo.flink.sql.demo;

public class SourceAndSinkSql {
  public final static String TABLE_NAME_TOKEN = "_TABLE_NAME_TOKEN";

  public final static String deleteIfExistsTable(String tableName) {
    return "DROP TABLE IF EXISTS _TABLE_NAME_TOKEN"
        .replace(TABLE_NAME_TOKEN, tableName);
  }


  public static String getSqlOfTable(String sql, String tableName) {
    return sql.replace(TABLE_NAME_TOKEN, tableName);
  }

  // -------------------------------- Generate -----------------------------------
  public final static String TOTAL_ROWS_TOKEN = "_TOTAL_ROWS_TOKEN";
  public final static String ROWS_PER_SECOND_TOKEN = "_ROWS_PER_SECOND_TOKEN";
  public final static String SOURCE_OF_GENERATE =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT PRIMARY KEY,\n" +
          "    pt  TIMESTAMP(3),\n" +
          "    author  ROW<first_name STRING, last_name STRING>,\n" +
          "    title   STRING,\n" +
          "    content STRING,\n" +
          "    num_like    BIGINT\n" +
          ") WITH (\n" +
          "    'connector' = 'datagen',\n" +
          "    'number-of-rows' = '_TOTAL_ROWS_TOKEN',\n" +
          "    'rows-per-second' = 'ROWS_PER_SECOND_TOKEN',\n" +
          "    'fields.id.kind' = 'sequence',\n" +
          "    'fields.id.start' = '1',\n" +
          "    'fields.id.end' = '_TOTAL_ROWS_TOKEN',\n" +
          "    'fields.author.first_name.length' = '2',\n" +
          "    'fields.author.last_name.length'='2',\n" +
          "    'fields.title.length' = '50',\n" +
          "    'fields.content.length' = '5000',\n" +
          "    'fields.num_like.min' = '0',\n" +
          "    'fields.num_like.max' = '10000'\n" +
          ")";

  public static String generateSourceSQL(long numberOfRows, long rowPerSecond) {
    return SOURCE_OF_GENERATE.replaceAll(TOTAL_ROWS_TOKEN, String.valueOf(numberOfRows))
        .replaceAll(ROWS_PER_SECOND_TOKEN, String.valueOf(rowPerSecond));
  }

  // -------------------------------- Kafka -----------------------------------
  public final static String KAFKA_BROKER_TOKEN = "_KAFKA_BROKER_TOKEN";
  public final static String KAFKA_TOPIC_TOKEN = "_KAFKA_TOPIC_TOKEN";
  public final static String KAFKA_PARALLELISM_TOKEN = "_KAFKA_PARALLELISM_TOKEN";
  public final static String KAFKA_GROUP_ID_TOKEN = "_KAFKA_GROUP_ID_TOKEN";

  public final static String SINK_TO_KAFKA =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    pt  TIMESTAMP(3),\n" +
          "    author  ROW<first_name STRING, last_name STRING>,\n" +
          "    title   STRING,\n" +
          "    content STRING,\n" +
          "    num_like    BIGINT\n" +
          ") WITH (\n" +
          "    'connector' = 'kafka',\n" +
          "    'topic' = '_KAFKA_TOPIC_TOKEN',\n" +
          "    'properties.bootstrap.servers' = '_KAFKA_BROKER_TOKEN',\n" +
          "    'key.fields' = 'id', \n" +
          "    'key.format' = 'raw', \n" +
          "    'value.format' = 'json', \n" +
          "    'sink.partitioner' = 'default',\n" +
//            "    'sink.semantic' = 'exactly-once', \n" +
//            在flink 内部进行运行时校验时，数据总量 <= Integer.MAX_VALUE* parallelism = 171+亿
          "    'sink.parallelism' = '_KAFKA_PARALLELISM_TOKEN'\n" +
//            "    'sink.parallelism' = '1',\n" +
//            "    'properties.security.protocol' = 'SASL_SSL',\n" +
//            "    'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" + //包括PLAIN, SCRAM-SHA-256
//            "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"z9xr2z4d\" password=\"xCPe0b4A5YUAyG_cqr81iSlHRQVvs6Cp\";'" +
          ")";
  public final static String SOURCE_OF_KAFKA =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    pt  TIMESTAMP(3),\n" +
          "    author  ROW<first_name STRING, last_name STRING>,\n" +
          "    title   STRING,\n" +
          "    content STRING,\n" +
          "    num_like    BIGINT\n" +
          ") WITH (\n" +
          "    'connector' = 'kafka',\n" +
          "    'topic' = '_KAFKA_TOPIC_TOKEN',\n" +
          "    'properties.bootstrap.servers' = '_KAFKA_BROKER_TOKEN',\n" +
          "    'properties.group.id' = '_KAFKA_GROUP_ID_TOKEN',\n" +
          //支持多种关于起始位置的offset 配置：'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'
          "    'scan.startup.mode' = 'earliest-offset', \n" +

          // scan.startup.mode配置为specific-offsets 时，可以配置offset从指定的offset开始读。
//          "    'scan.startup.specific-offsets' = 'partition:0,offset:42;partition:1,offset:300', \n" +

          // scan.topic-partition-discovery.interval 用于发现动态创建的 topics 和 partition。
//          "    'scan.topic-partition-discovery.interval' = '60000', \n" +

          "    'key.fields' = 'id', \n" +
          "    'key.format' = 'raw', \n" +
          "    'value.format' = 'json' \n" +
//            "    'properties.security.protocol' = 'SASL_SSL',\n" +
//            "    'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" + //包括PLAIN, SCRAM-SHA-256
//            "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"z9xr2z4d\" password=\"xCPe0b4A5YUAyG_cqr81iSlHRQVvs6Cp\";'" +
          ")";

  public static String kafkaSourceSQL(String bootstrapServer, String topics, String groupId) {
    return SOURCE_OF_KAFKA.replace(KAFKA_BROKER_TOKEN, bootstrapServer)
        .replace(KAFKA_TOPIC_TOKEN, topics)
        .replace(KAFKA_GROUP_ID_TOKEN, groupId);
  }

  public static String kafkaSinkSQL(String bootstrapServer, String topic, int parallelism) {
    return SOURCE_OF_KAFKA.replace(KAFKA_BROKER_TOKEN, bootstrapServer)
        .replace(KAFKA_TOPIC_TOKEN, topic)
        .replace(KAFKA_PARALLELISM_TOKEN, String.valueOf(parallelism));
  }


}
