package win.hgfdodo.flink.sql.demo;

public class TransformSql {
  public final static String TOPN_TOKEN = "_TOPN_TOKEN";

  public static final String DEFAULT_SELECT = "select * from _TABLE_NAME_TOKEN";

  public static String selectAll(String tableName) {
    return DEFAULT_SELECT.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, tableName);
  }

  // -------------------------------- TopN -----------------------------------

  public static final String topN =
      "select id, author.first_name as first_name, author.last_name as last_name, num_like \n" +
          "FROM ( \n" +
          "    select id, author, num_like, \n" +
          "        ROW_NUMBER() over ( \n" +
          "            order by num_like desc \n" +
          "        ) as rownum \n" +
          "    FROM _TABLE_NAME_TOKEN \n" +
          ") where rownum <= _TOPN_TOKEN \n";

  public static final String topNInWindow =
      "select id, author.first_name as first_name, author.last_name as last_name, num_like, pt, window_start, window_end \n" +
          "FROM (\n" +
          "    select id, author, num_like, pt, window_start, window_end,\n" +
          "        ROW_NUMBER() over (\n" +
          "            partition by window_start, window_end order by num_like desc\n" +
          "        ) as rownum\n" +
          "    FROM TABLE( TUMBLE(TABLE _TABLE_NAME_TOKEN, DESCRIPTOR(pt), INTERVAL '1' DAYS))\n" +
          ") where rownum <=_TOPN_TOKEN \n";


  /**
   * 所有数据的 topN
   *
   * @param table
   * @param topn
   * @return
   */
  public static String topNNumLikeArticleSQL(String table, int topn) {
    return topN.replaceAll(SourceAndSinkSql.TABLE_NAME_TOKEN, table)
        .replace(TOPN_TOKEN, String.valueOf(topn));
  }

  /**
   * 每日topn的信息
   *
   * @param table
   * @param topn
   * @return
   */
  public static String topNNumLikeInWindowSQL(String table, int topn) {
    return topNInWindow.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table)
        .replace(TOPN_TOKEN, String.valueOf(topn));
  }
  // -------------------------------- order by -----------------------------------

  public final static String ORDER_SQL =
      "select id, pt, author, title, content, num_like \n" +
          "from _TABLE_NAME_TOKEN \n" +
          "order by pt asc \n";

  public static String orderByPt(String table) {
    return ORDER_SQL.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table);
  }


  // -------------------------------- distinct -----------------------------------
  public static final String distinct =
      "select * " +
          "FROM (" +
          "    select id, author, num_like," +
          "        ROW_NUMBER() over (" +
          "            PARTITION BY  author" +
          "            order by pt desc" +
          "        ) as rownum" +
          "    FROM _TABLE_NAME_TOKEN" +
          ") where (rownum <= 1 | rownum = 1 | rownum < 2) \n";

  public static String distinct(String table) {
    return distinct.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table);
  }

  public static final String distinctInWindow =
      "select * " +
          "FROM (" +
          "    select id, author, num_like, window_start, window_end," +
          "        ROW_NUMBER() over (" +
          "            PARTITION BY windows_start, window_end, author" +
          "            order by pt desc" +
          "        ) as rownum" +
          "    FROM TABLE( TUMBLE(TABLE _TABLE_NAME_TOKEN, DESCRIPTOR(pt), INTERVAL '1' DAYS))" +
          ") where (rownum <= 1 | rownum = 1 | rownum < 2) \n";

  public static String getDistinctInWindow(String table) {
    return distinctInWindow.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table);
  }

  // ----------------------------- group by count ---------------------------------
  public static final String groupCount =
      "select author, count(rownum) as published " +
          "FROM (" +
          "    select  author," +
          "        ROW_NUMBER() over (" +
          "            PARTITION BY author" +
          "        ) as rownum" +
          "    FROM _TABLE_NAME_TOKEN" +
          ")\n";

  public static String groupCount(String table) {
    return groupCount.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table);
  }
}
