package win.hgfdodo.flink.sql.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Simple {

  public static List<Order> generateOrders() {
    Random random = new Random();
    final List<String> person = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu", "ba");
    final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    LocalDateTime dateTime = LocalDateTime.now();

    List<Order> orders = new ArrayList<>();
    int n = 1000;
    for (int i = 0; i < n; i++) {
      int index = random.nextInt(person.size());
      LocalDateTime some = dateTime.plusMinutes(random.nextInt(100));

      orders.add(new Order(1, person.get(index), dateTimeFormatter.format(some), random.nextInt(300) + random.nextDouble()));
    }

    return orders;
  }

  public static void main(String[] args) throws Exception {
    exec2();
  }

  public static void exec1() throws Exception {
    // 从DataStream 中 构造Table
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment env = StreamTableEnvironment.create(sEnv);

    List<Order> orders = generateOrders();
    DataStreamSource<Order> orderDataStream = sEnv.fromElements(orders.toArray(new Order[orders.size()]));
    Table table = env.fromDataStream(orderDataStream);

    env.createTemporaryView("t_order", table);

    String sql = "select userName, sum(money) totalMoney, max(money) maxMoney, min(money) minMoney, count(1) totalCount " +
        "from t_order group by userName";
    String explain = env.explainSql(sql, ExplainDetail.CHANGELOG_MODE);
    System.out.println(explain);
    env.executeSql(sql)
        .print();
  }

  public static String[] generateLines() {
    Random random = new Random();
    int n = random.nextInt(100);
    String[] generatedLiens = new String[n];
    List<String> word = Arrays.asList("aa", "as", "im", "chat", "go", "pro");
    for (int i = 0; i < n; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < random.nextInt(5) + 1; j++) {
        sb.append(word.get(random.nextInt(word.size())));
        sb.append(" ");
      }
      generatedLiens[i] = sb.toString();
    }
    return generatedLiens;
  }

  public static void exec2() throws Exception {
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> socketStream = sEnv.fromElements(generateLines());
    socketStream.flatMap(new FlatMapFunction<String, String>() {
          @Override
          public void flatMap(String value, Collector<String> out)
              throws Exception {
            for (String word : value.split(" ")) {
              out.collect(word);
            }
          }
        })
        .filter(new FilterFunction<String>() {
          @Override
          public boolean filter(String s) throws Exception {
            if (StringUtils.isNoneEmpty(s)) {
              return true;
            }
            return false;
          }
        })
        .map(new MapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> map(String s) throws Exception {
            return Tuple2.of(s, 1);
          }
        })
        .keyBy(v -> v.getField(0))
        .sum(1).print();
    sEnv.execute();
  }

}
