package win.hgfdodo.flink.sql.demo;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class App {
  public static void main(String[] args) {
    EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(envSettings);

    // source
    tableEnvironment.executeSql(
        "CREATE TABLE HGF_ORDER (\n" +
            "        `id` INT,\n" +
            "        `create_time` STRING,\n" +
            "        `money` DOUBLE,\n" +
            "        `user_name` STRING\n" +
            "    ) WITH (\n" +
            "        'connector'='jdbc',\n" +
            "        'url' = 'jdbc:mysql://10.208.63.130:30201/source?encoding=UTF-8',\n" +
            "        'table-name'='hgf_order',\n" +
            "        'username'='root',\n" +
            "        'password' = 'changeme'" +
            "    )");

    // sink to file
    tableEnvironment.executeSql(
        "CREATE TABLE OUTPUT(\n" +
            "        `username` STRING,\n" +
            "        `max_money` FLOAT,\n" +
            "        `min_money` FLOAT,\n" +
            "        `total_money` FLOAT\n" +
            "        )WITH(\n" +
            "        'connector' = 'filesystem',\n" +
            "        'path' = 'file:///tmp/output.csv',\n" +
            "        'format' = 'csv',\n" +
            "        'sink.partition-commit.delay' = '1 m',\n" +
            "        'sink.partition-commit.policy.kind' = 'success-file'\n" +
            "    )");
    Table source = tableEnvironment.from("HGF_ORDER");
    source.groupBy($("user_name"))
            .select(
                $("user_name").as("username"),
                $("money").sum().cast(DataTypes.FLOAT()).as("total_money"),
                $("money").max().cast(DataTypes.FLOAT()).as("max_money"),
                $("money").sum().cast(DataTypes.FLOAT()).as("min_money")
            ).executeInsert("OUTPUT");
  }
}
