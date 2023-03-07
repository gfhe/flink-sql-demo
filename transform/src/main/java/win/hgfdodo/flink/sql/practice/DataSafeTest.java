package win.hgfdodo.flink.sql.practice;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.flink.sql.demo.PipelineEngine;
import win.hgfdodo.flink.sql.demo.SourceAndSinkSql;
import win.hgfdodo.flink.sql.demo.TransformSql;

/**
 * 保证数据安全性，Exact-once：不丢失，不重复， 以kafka为例子；
 * <p>
 * 测试方案：job manager 故障恢复时，能否保证数据不丢、不多。
 * <p>
 * 故障的可能的时机：
 * 1. 读入，但是未写入kafka；
 * 2. 写入kafka 后，checkpoint 提交前。
 * 3. 写入kafka，checkpoint 提交，但是checkpoint 两阶段提交没完成。
 * 4. checkpoint 二阶段提交完成。
 * <p>
 * 1. 生成数据写入kafka， 构造 data topic数据。
 * 2. data topic 作为source
 * 3. 增加UDF，慢写入数据，记录处理日志。
 * 4. 处理后的数据写入sink topic
 */
public class DataSafeTest {
  private final static Logger log = LoggerFactory.getLogger(DataSafeTest.class);
  private final static String KAFKA_BROKER_SERVER = "172.22.0.37:9092";
  private final static String sourceDataTopic = "data_safe_source";
  private final static String destDataTopic = "data_safe_sink";
  private final static String GROUP_ID = "safe_test_consumer";
  private final static int parellelism = 6;

  public static void main(String[] args) {
//    generateAndSinkTestData();
    consumeData();
  }

  public static void generateAndSinkTestData() {
    String sourceTableName = "DataGenerated";
    String genSourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.generateSourceDDL(100000, 3000, 1000 * 60 * 60 * 24), sourceTableName);
    String kafkaSinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(KAFKA_BROKER_SERVER, sourceDataTopic, parellelism), sourceDataTopic);
    PipelineEngine.execution(sourceTableName, genSourceDDL, TransformSql.selectAll(sourceTableName), kafkaSinkDDL, sourceDataTopic);
  }

  public static void consumeData() {
    int processParellelism = 1;
    String kafkaSourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceDDL(KAFKA_BROKER_SERVER, sourceDataTopic, GROUP_ID), sourceDataTopic);
    String kafkaSinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(KAFKA_BROKER_SERVER, destDataTopic, processParellelism), destDataTopic);

    log.info("Source DDL: {}", kafkaSinkDDL);
    log.info("Sink DDL: {}", kafkaSinkDDL);

    StreamExecutionEnvironment streamSetting = StreamExecutionEnvironment.getExecutionEnvironment();
    streamSetting.enableCheckpointing(1000);
    streamSetting.getCheckpointConfig().setCheckpointStorage("s3://flink/checkpoint/tmp1");
    streamSetting.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamSetting.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    streamSetting.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    // 当作业异常时，保留作业的checkpoint，只有作业canceling时删除checkpoint。
    streamSetting.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamSetting, envSettings);

    //准备表
    tableEnvironment.executeSql(kafkaSourceDDL);
    tableEnvironment.executeSql(kafkaSinkDDL);

    //载入udf
    tableEnvironment.createTemporarySystemFunction("TimeConsuming", TimeConsumingUDF.class);

    Table source = tableEnvironment.from(sourceDataTopic);
    String transformSql = String.format("insert into %s " +
            "select id, pt, author, TimeConsuming(title) as title, content, num_like from %s",
        destDataTopic,
        sourceDataTopic
    );
//    String transformSql = String.format("select id, pt, author, TimeConsuming(title) as title, content, num_like from %s limit 10", sourceDataTopic);
//    tableEnvironment.sqlQuery(transformSql).execute().print();
    tableEnvironment.executeSql(transformSql);
  }
}
