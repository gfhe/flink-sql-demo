package win.hgfdodo.flink.sql.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class EngineContextMain {
  final static String brokers = "172.22.0.37:9092";

  final static EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
  final static TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);


  public static void main(String[] args) {
    String topics = "mock_data_20_billion";
    String group = "test_top_num_like";
    String sourceTable = "InputSource";
    String sourceSql = SourceAndSinkSql.getSqlOfTable(SourceAndSinkSql.kafkaSourceSQL(brokers, topics, group), sourceTable);
    String topNTransformSql = TransformSql.topNNumLikeArticleSQL(sourceTable, 10);
    pipeline(sourceTable, sourceSql, topNTransformSql);
  }

  static void pipeline(String sourceTableName, String sourceSql, String transformSql) {
    System.out.println("source:" + sourceSql);
    System.out.println("transform:" + transformSql);

    tableEnvironment.executeSql(sourceSql);

    Table input = tableEnvironment.from(sourceTableName);
    tableEnvironment.executeSql(transformSql).print();
//    tableEnvironment.executeSql("select * from "+sourceTableName+" limit 10").print();
//    Table resultTable = tableEnvironment.sqlQuery(transformSql);
//    resultTable.execute().print();
  }
}
