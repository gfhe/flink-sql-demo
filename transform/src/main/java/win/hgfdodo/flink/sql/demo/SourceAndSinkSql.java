package win.hgfdodo.flink.sql.demo;

public class SourceAndSinkSql {
  public final static String TABLE_NAME_TOKEN = "_TABLE_NAME_TOKEN";
  public final static String WATERMARK_TOKEN = "_WATERMARK_TOKEN";

  public final static String SINK_TO_TABLE_TAMPLATE = "INSERT INTO %s %s";

  /**
   * 获取基于transform 的sink sql
   *
   * @param transformSQL  transform sql
   * @param sinkTableName sink to this table
   * @return sink sql
   */
  public static String sinkSQLFromTransform(String transformSQL, String sinkTableName) {
    return String.format(SINK_TO_TABLE_TAMPLATE, sinkTableName, transformSQL);
  }

  public final static String dropIfExistsTable(String tableName) {
    return "DROP TABLE IF EXISTS _TABLE_NAME_TOKEN"
        .replace(TABLE_NAME_TOKEN, tableName);
  }


  public static String getSqlOfTable(String sql, String tableName) {
    return sql.replace(TABLE_NAME_TOKEN, tableName);
  }

  // -------------------------------- Generate -----------------------------------
  public final static String TOTAL_ROWS_TOKEN = "_TOTAL_ROWS_TOKEN";
  public final static String ROWS_PER_SECOND_TOKEN = "_ROWS_PER_SECOND_TOKEN";
  public final static String DATA_DURATION_TOKEN_MS = "_DATA_DURATION_TOKEN";
  public final static String DDL_OF_GENERATE_SOURCE =
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
          "    'rows-per-second' = '_ROWS_PER_SECOND_TOKEN',\n" +
          "    'fields.id.kind' = 'sequence',\n" +
          "    'fields.id.start' = '1',\n" +
          "    'fields.id.end' = '_TOTAL_ROWS_TOKEN',\n" +
          "    'fields.pt.max-past' = '_DATA_DURATION_TOKEN',\n" +
          "    'fields.author.first_name.length' = '2',\n" +
          "    'fields.author.last_name.length'='2',\n" +
          "    'fields.title.length' = '50',\n" +
          "    'fields.content.length' = '5000',\n" +
          "    'fields.num_like.min' = '0',\n" +
          "    'fields.num_like.max' = '10000'\n" +
          ")";

  public final static String DDL_OF_GENERATE_PT_ASC_SOURCE =
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
          "    'rows-per-second' = '_ROWS_PER_SECOND_TOKEN',\n" +
          "    'fields.id.kind' = 'sequence',\n" +
          "    'fields.id.start' = '1',\n" +
          "    'fields.id.end' = '_TOTAL_ROWS_TOKEN',\n" +
          "    'fields.pt.max-past' = '_DATA_DURATION_TOKEN',\n" +
          "    'fields.author.first_name.length' = '2',\n" +
          "    'fields.author.last_name.length'='2',\n" +
          "    'fields.title.length' = '50',\n" +
          "    'fields.content.length' = '5000',\n" +
          "    'fields.num_like.min' = '0',\n" +
          "    'fields.num_like.max' = '10000'\n" +
          ")";

  public static String generateSourceDDL(long numberOfRows, long rowsPerSecond, long maxPathMs) {
    return DDL_OF_GENERATE_SOURCE.replaceAll(TOTAL_ROWS_TOKEN, String.valueOf(numberOfRows))
        .replaceAll(ROWS_PER_SECOND_TOKEN, String.valueOf(rowsPerSecond))
        .replace(DATA_DURATION_TOKEN_MS, String.valueOf(maxPathMs));
  }

  // -------------------------------- CSV  -----------------------------------
  public final static String DDL_OF_CSV_FILE =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    author  ROW<first_name STRING, last_name STRING>,\n" +
          "    num_like    BIGINT\n" +
          ") WITH (\n" +
          "    'connector' = 'filesystem',\n" +
          "    'path' = 'file:///tmp/test',\n" +
          "    'format' = 'csv'\n" +
          ")";

  public final static String csvSinkDDL() {
    return DDL_OF_CSV_FILE;
  }

  // -------------------------------- JDBC  -----------------------------------

  public final static String JDBC_TABLE_NAME = "_JDBC_TABLE_NAME";

  public final static String DDL_OF_JDBC =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    first_name STRING,\n" +
          "    last_name STRING,\n" +
          "    num_like    BIGINT," +
          "    PRIMARY KEY (id) NOT ENFORCED\n" +
          ") WITH (\n" +
          "    'connector' = 'jdbc',\n" +
          "    'url' = 'jdbc:mysql://10.208.63.130:30201/sink?encoding=UTF-8',\n" +
          "    'driver' = 'com.mysql.jdbc.Driver',\n" +
          "    'username' = 'root',\n" +
          "    'password' = 'changeme',\n" +
          "    'table-name' = '_JDBC_TABLE_NAME'\n" +
          ")";
  public final static String DDL_OF_JDBC_WINDOW =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    first_name STRING,\n" +
          "    last_name STRING,\n" +
          "    num_like    BIGINT," +
          "    pt    TIMESTAMP," +
          "    window_start TIMESTAMP," +
          "    window_end   TIMESTAMP," +
          "    PRIMARY KEY (id) NOT ENFORCED\n" +
          ") WITH (\n" +
          "    'connector' = 'jdbc',\n" +
          "    'url' = 'jdbc:mysql://10.208.63.130:30201/sink?encoding=UTF-8',\n" +
          "    'driver' = 'com.mysql.jdbc.Driver',\n" +
          "    'username' = 'root',\n" +
          "    'password' = 'changeme',\n" +
          "    'table-name' = '_JDBC_TABLE_NAME'\n" +
          ")";

  public final static String jdbcSinkDDL(String jdbcTable) {
    return DDL_OF_JDBC.replace(JDBC_TABLE_NAME, jdbcTable);
  }
  public final static String jdbcSinkDDLWindow(String jdbcTable) {
    return DDL_OF_JDBC_WINDOW.replace(JDBC_TABLE_NAME, jdbcTable);
  }

  // -------------------------------- Kafka -----------------------------------
  public final static String KAFKA_BROKER_TOKEN = "_KAFKA_BROKER_TOKEN";
  public final static String KAFKA_TOPIC_TOKEN = "_KAFKA_TOPIC_TOKEN";
  public final static String KAFKA_PARALLELISM_TOKEN = "_KAFKA_PARALLELISM_TOKEN";
  public final static String KAFKA_GROUP_ID_TOKEN = "_KAFKA_GROUP_ID_TOKEN";

  public final static String DDL_OF_KAFKA_SINK =
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
  public final static String DDL_OF_KAFKA_SOURCE =
      "create table _TABLE_NAME_TOKEN(\n" +
          "    id  BIGINT,\n" +
          "    pt  TIMESTAMP(3),\n" +
          "    author  ROW<first_name STRING, last_name STRING>,\n" +
          "    title   STRING,\n" +
          "    content STRING,\n" +
          "    num_like    BIGINT\n" +
          "    _WATERMARK_TOKEN\n" +
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

  public static String kafkaSourceDDL(String bootstrapServer, String topics, String groupId) {
    return DDL_OF_KAFKA_SOURCE.replace(KAFKA_BROKER_TOKEN, bootstrapServer)
        .replace(KAFKA_TOPIC_TOKEN, topics)
        .replace(KAFKA_GROUP_ID_TOKEN, groupId)
        .replace(WATERMARK_TOKEN, ", WATERMARK FOR pt as pt - INTERVAL '1' DAYS \n");
  }

  public static String kafkaSinkDDL(String bootstrapServer, String topic, int parallelism) {
    return DDL_OF_KAFKA_SINK.replace(KAFKA_BROKER_TOKEN, bootstrapServer)
        .replace(KAFKA_TOPIC_TOKEN, topic)
        .replace(WATERMARK_TOKEN, "")
        .replace(KAFKA_PARALLELISM_TOKEN, String.valueOf(parallelism));
  }


}
