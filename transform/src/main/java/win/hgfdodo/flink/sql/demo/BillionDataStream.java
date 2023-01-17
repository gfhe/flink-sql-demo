package win.hgfdodo.flink.sql.demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class BillionDataStream {
  public static void main(String[] args) {
    EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);

    // source
    tableEnvironment.executeSql(
        "create table mock_data_20_billion(\n" +
            "    id  BIGINT PRIMARY KEY,\n" +
            "    pt  TIMESTAMP(3),\n" +
            "    author  ROW<first_name STRING, last_name STRING>,\n" +
            "    title   STRING,\n" +
            "    content STRING,\n" +
            "    num_like    BIGINT\n" +
            ") WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'number-of-rows' = '100000000',\n" +
            "    'rows-per-second' = '20000',\n" +
            "    'fields.id.kind' = 'sequence',\n" +
            "    'fields.id.start' = '1',\n" +
            "    'fields.id.end' = '100000000',\n" +
            "    'fields.author.first_name.length' = '3',\n" +
            "    'fields.author.last_name.length'='10',\n" +
            "    'fields.title.length' = '50',\n" +
            "    'fields.content.length' = '5000',\n" +
            "    'fields.num_like.min' = '0',\n" +
            "    'fields.num_like.max' = '10000'\n" +
            ")");

    // sink to file
    tableEnvironment.executeSql(
        "create table sink_kafka_stream(\n" +
            "    id  BIGINT,\n" +
            "    pt  TIMESTAMP(3),\n" +
            "    author  ROW<first_name STRING, last_name STRING>,\n" +
            "    title   STRING,\n" +
            "    content STRING,\n" +
            "    num_like    BIGINT\n" +
            ") WITH (\n" +
            "    'connector' = 'kafka',\n" +
            "    'topic' = 'mock_data_billion_stream',\n" +
            "    'properties.bootstrap.servers' = '172.22.0.37:9092',\n" +
            "    'key.fields' = 'id', \n" +
            "    'key.format' = 'raw', \n" +
            "    'value.format' = 'json', \n" +
            "    'sink.partitioner' = 'default',\n" +
//            "    'sink.semantic' = 'exactly-once', \n" +
//            在flink 内部进行运行时校验时，数据总量 <= Integer.MAX_VALUE* parallelism = 171+亿
            "    'sink.parallelism' = '6'\n" +
//            "    'sink.parallelism' = '1',\n" +
//            "    'properties.security.protocol' = 'SASL_SSL',\n" +
//            "    'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" + //包括PLAIN, SCRAM-SHA-256
//            "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"z9xr2z4d\" password=\"xCPe0b4A5YUAyG_cqr81iSlHRQVvs6Cp\";'" +
            ")");

    Table source = tableEnvironment.from("mock_data_20_billion");
    source.executeInsert("sink_kafka_stream");
  }
}
