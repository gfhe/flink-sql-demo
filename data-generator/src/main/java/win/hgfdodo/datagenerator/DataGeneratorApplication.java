package win.hgfdodo.datagenerator;

import com.github.jsonzou.jmockdata.JMockData;
import com.github.jsonzou.jmockdata.MockConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import win.hgfdodo.datagenerator.domain.Order;
import win.hgfdodo.datagenerator.repositories.OrderRepository;

import java.sql.Array;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableJpaRepositories
public class DataGeneratorApplication implements CommandLineRunner {
  @Autowired
  OrderRepository orderRepository;
  private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private final MockConfig mockConfig = new MockConfig()
      .subConfig(Order.class, "userName")
      .stringSeed("a", "b", "c", "d")
      .sizeRange(2, 3)

      .globalConfig()
      .excludes("id");

  public static void main(String[] args) {
    SpringApplication.run(DataGeneratorApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < 5; i++) {
            Order order = JMockData.mock(Order.class, mockConfig);
            order.setId(null);
            order.setCreateTime(formatter.format(LocalDateTime.now()));
            orderRepository.save(order);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, 1, 5, TimeUnit.SECONDS);
    Thread.sleep(100000);
  }
}
