package win.hgfdodo.flink.sql.demo;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PipelineEngine {
  private final static Logger log = LoggerFactory.getLogger(PipelineEngine.class);

  final static String brokers = "172.22.0.37:9092";
  final static String topics = "mock_data_20_billion";
  final static String group = "test_top_num_like1";
  final static Integer parellel = 8;

  final static ExecutorService backgroundThread = Executors.newSingleThreadExecutor();

  final static EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
  final static TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);

  public static void main(String[] args) {
//    prod();
    dev();
  }

  public static void prod() {
    // 生成数据
//    generateData("generate_data", "mock_data", null, 100000000, 10000);
    // 测试topn
//    testTopNWindow(3, "mock_data", null, "sink_data");
    testTopN(10, "mock_data", null, "sink_total_top_n");
  }

  public static void dev() {
//    generateData("dev_generate_data", "dev_mock_data", null, 1000, 10000);

    // 每日点赞top3
//    testTopNWindow(3, "dev_mock_data", null, "sink_day_top_n");
    testTopN(3, "dev_mock_data", null, "sink_total_top_n");
//    orderBy("dev_mock_data", "dev_mock_data_ordered", null, null);
  }

  public static void generateDataTest() {
    String table = "test";
    SourceAndSinkSql.dropIfExistsTable(table);
    String sourceSql = SourceAndSinkSql.generateSourceDDL(10, 10, 1000 * 3600 * 24);
    sourceSql = SourceAndSinkSql.getSqlOfTable(sourceSql, table);

    tableEnvironment.executeSql(sourceSql);

    Table input = tableEnvironment.from(table);
    tableEnvironment.executeSql("select  * from test").print();
  }

  public static void generateDataRandom(String sourceTable, String sinkTable, String topic, long rows, long rowsPerSecond) {
    SourceAndSinkSql.dropIfExistsTable(sourceTable);
    SourceAndSinkSql.dropIfExistsTable(sinkTable);

    if (topic == null) {
      topic = sinkTable;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.generateSourceDDL(rows, rowsPerSecond, 1000 * 3600 * 24 * 3), sourceTable);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(brokers, topic, parellel), sinkTable);

    execution(sourceTable, sourceDDL, TransformSql.selectAll(sourceTable), sinkDDL, sinkTable);
  }

  public static void generateDataPtOrder(String sourceTable, String sinkTable, String topic, long rows, long rowsPerSecond) {
    SourceAndSinkSql.dropIfExistsTable(sourceTable);
    SourceAndSinkSql.dropIfExistsTable(sinkTable);

    if (topic == null) {
      topic = sinkTable;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.generateSourceDDL(rows, rowsPerSecond, 1000 * 3600 * 24 * 3), sourceTable);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(brokers, topic, parellel), sinkTable);


    String sinkSQL = "insert into %s \n" +
        "select id, 1675307161000+ row_num as pt, author, title, content, num_like, \n" +
        "  ROW_NUMBER() over () as row_num\n" +
        "from %s \n";
    log.info("[SQL-sink]:\t{}", sinkSQL);
//
//    tableEnvironment.executeSql(sourceDDL);
//    tableEnvironment.executeSql(sinkDDL);
//
//    Table input = tableEnvironment.from(sourceTableName);
//    TableResult tableResult = tableEnvironment.executeSql(sinkSQL);
  }

  /**
   * 每日点赞top3的文章
   * kafka -> 统计 -> mysql
   * <p>
   * 测试前需要创建mysql db table
   * create table sink_data(
   * id bigint primary key,
   * first_name varchar(255),
   * last_name varchar(255),
   * num_like bigint
   * )
   *
   * @param topic
   * @param sourceTable
   * @param sinkTable
   */
  public static void testTopNWindow(int topn, String topic, String sourceTable, String sinkTable) {
    if (sourceTable == null) {
      sourceTable = topic;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceDDL(brokers, topic, group), sourceTable);
    String topNTransformSql = TransformSql.topNNumLikeInWindowSQL(sourceTable, topn);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.jdbcSinkDDLWindow(sinkTable), sinkTable);

    execution(sourceTable, sourceDDL, topNTransformSql, sinkDDL, sinkTable);
  }

  /**
   * 点赞top3的文章
   * kafka -> 统计 -> mysql
   * <p>
   * 测试前需要创建mysql db table
   * create table sink_data(
   * id bigint primary key,
   * first_name varchar(255),
   * last_name varchar(255),
   * num_like bigint
   * )
   *
   * @param topic
   * @param sourceTable
   * @param sinkTable
   */
  public static void testTopN(int topn, String topic, String sourceTable, String sinkTable) {
    if (sourceTable == null) {
      sourceTable = topic;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceDDL(brokers, topic, group), sourceTable);
    String topNTransformSql = TransformSql.topNNumLikeArticleSQL(sourceTable, topn);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.jdbcSinkDDL(sinkTable), sinkTable);

    execution(sourceTable, sourceDDL, topNTransformSql, sinkDDL, sinkTable);
  }

  /**
   * 依据pt 排序输出
   *
   * @param srcTopic
   * @param destTopic
   * @param sourceTable
   * @param sinkTable
   */
  public static void orderBy(String srcTopic, String destTopic, String sourceTable, String sinkTable) {
    if (sourceTable == null) {
      sourceTable = srcTopic;
    }
    if (sinkTable == null) {
      sinkTable = destTopic;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceDDL(brokers, srcTopic, group), sourceTable);
    String orderByTransform = TransformSql.orderByPt(sourceTable);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(brokers, destTopic, 8), sinkTable);

    execution(sourceTable, sourceDDL, orderByTransform, sinkDDL, sinkTable);
  }

  public static void execution(String sourceTableName, String sourceDDL, String transformSQL, String sinkDDL, String sinkTableName) {
    log.info("[table]: source={}, sink={}", sourceTableName, sinkTableName);
    log.info("[SQL-DLL-source]:\t{}", sourceDDL);
    log.info("[SQL-DLL-sink]:\t{}", sinkDDL);
    log.info("[SQL-transform]:\t{}", transformSQL);
    String sinkSQL = SourceAndSinkSql.sinkSQLFromTransform(transformSQL, sinkTableName);
    log.info("[SQL-sink]:\t{}", sinkSQL);

    tableEnvironment.executeSql(sourceDDL);
    tableEnvironment.executeSql(sinkDDL);

    Table input = tableEnvironment.from(sourceTableName);
    TableResult tableResult = tableEnvironment.executeSql(sinkSQL);
    Optional<JobClient> jobClientOptional = tableResult.getJobClient();
    final JobClient jobClient = jobClientOptional.get();
    log.info("!!! submit success, job id={}", jobClient.getJobID());
    printJobStatus(jobClient);

    switch (tableResult.getResultKind()) {
      case SUCCESS:
        log.info("sql execute success");
        break;
      case SUCCESS_WITH_CONTENT:
        log.info("sql execute success with content");
        break;
    }
  }

  public static void printJobStatus(final JobClient jobClient) {
    backgroundThread.execute(() -> {
      try {
        while (true) {
          JobStatus jobStatus = jobClient.getJobStatus().get(1, TimeUnit.SECONDS);
          log.debug("[job-status]: {}, terminated:{}, global terminated: {},", jobStatus.name(), jobStatus.isTerminalState(), jobStatus.isGloballyTerminalState());
          Thread.sleep(3000);
        }
      } catch (Exception e) {
        log.error("print job status error", e);
      }
    });
  }
}
