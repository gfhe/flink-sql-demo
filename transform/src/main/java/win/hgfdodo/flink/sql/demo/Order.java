package win.hgfdodo.flink.sql.demo;

import org.apache.flink.table.annotation.DataTypeHint;

public class Order {
  @DataTypeHint("INT")
  private Integer id;

  @DataTypeHint("STRING")
  private String userName;

  @DataTypeHint("STRING")
  private String createTime;

  @DataTypeHint("DOUBLE")
  private Double money;

  public Order() {
  }

  public Order(Integer id, String userName, String createTime, Double money) {
    this.id = id;
    this.userName = userName;
    this.createTime = createTime;
    this.money = money;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public Double getMoney() {
    return money;
  }

  public void setMoney(Double money) {
    this.money = money;
  }

  @Override
  public String toString() {
    return "Order{" +
        "id=" + id +
        ", userName='" + userName + '\'' +
        ", createTime='" + createTime + '\'' +
        ", money=" + money +
        '}';
  }
}
