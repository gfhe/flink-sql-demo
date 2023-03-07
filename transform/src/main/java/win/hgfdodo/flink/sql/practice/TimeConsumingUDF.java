package win.hgfdodo.flink.sql.practice;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeConsumingUDF extends ScalarFunction {
  private Random random = new Random();

  private AtomicInteger counter = new AtomicInteger(0);
  public static final Logger log = LoggerFactory.getLogger(TimeConsumingUDF.class);

  public String eval(String in) {
    int count = counter.getAndIncrement();
    if(count == 0){
      log.info("in time consuming udf");
    }
    int sleepms = random.nextInt(3);
    try {
      Thread.sleep(sleepms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return in;
  }
}
