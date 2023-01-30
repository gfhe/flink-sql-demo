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
  final static String group = "test_top_num_like";
  final static Integer parellel = 8;

  final static ExecutorService backgroundThread = Executors.newSingleThreadExecutor();

  final static EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
  final static TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);


  public static void main(String[] args) {
    generateData("generate_data", "mock_data", null, 100, 10000);
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

  public static void generateData(String sourceTable, String sinkTable, String topic, long rows, long rowsPerSecond) {
    SourceAndSinkSql.dropIfExistsTable(sourceTable);
    SourceAndSinkSql.dropIfExistsTable(sinkTable);

    if (topic == null) {
      topic = sinkTable;
    }

    String sourceDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.generateSourceDDL(rows, rowsPerSecond, 1000 * 3600 * 24 * 3), sourceTable);
    String sinkDDL = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSinkDDL(brokers, topic, parellel), sinkTable);

    execution(sourceTable, sourceDDL, TransformSql.selectAll(sourceTable), sinkDDL, sinkTable);
  }

  public static void testTopN(String sourceTable, String topic) {
    String sourceSql = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceDDL(brokers, topic, group), sourceTable);
    String topNTransformSql = TransformSql.topNNumLikeArticleSQL(sourceTable, 10);
    debugTransform(sourceTable, sourceSql, topNTransformSql);
  }

  public static void execution(String sourceTableName, String sourceDDL, String transformSQL, String sinkDDL, String sinkTableName) {
    log.debug("[table]: source={}, sink={}", sourceTableName, sinkTableName);
    log.debug("[SQL-DLL-source]:\t{}", sourceDDL);
    log.debug("[SQL-DLL-sink]:\t{}", sinkDDL);
    log.debug("[SQL-transform]:\t{}", transformSQL);
    String sinkSQL = SourceAndSinkSql.sinkSQLFromTransform(transformSQL, sinkTableName);
    log.debug("[SQL-sink]:\t{}", sinkSQL);

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

  public static void debugTransform(String sourceTableName, String sourceSql, String transformSql) {
    System.out.println("source:" + sourceSql);
    System.out.println("transform:" + transformSql);

    tableEnvironment.executeSql(sourceSql);

    Table input = tableEnvironment.from(sourceTableName);
    tableEnvironment.executeSql(transformSql).print();
//    tableEnvironment.executeSql("select * from "+sourceTableName+" limit 10").print();
//    Table resultTable = tableEnvironment.sqlQuery(transformSql);
//    resultTable.execute().print();
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
