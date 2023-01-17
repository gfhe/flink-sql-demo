package win.hgfdodo.flink.sql.demo;

public class TransformSql {
  public final static String TOPN_TOKEN = "_TOPN_TOKEN";

  // -------------------------------- TopN -----------------------------------

  public static final String topN =
      "select id, author, num_like " +
          "FROM (" +
          "    select id, author, num_like," +
          "        ROW_NUMBER() over (" +
          "            order by num_like desc" +
          "        ) as rownum" +
          "    FROM _TABLE_NAME_TOKEN" +
          ") where rownum <= _TOPN_TOKEN \n";
  public static final String topNInWindow =
      "select id, author, num_like " +
          "FROM (" +
          "    select id, author, num_like," +
          "        ROW_NUMBER() over (" +
          "            order by num_like desc" +
          "        ) as rownum" +
          "    FROM TABLE( TUMBLE(TABLE _TABLE_NAME_TOKEN, DESCRIPTOR(pt), INTERVAL '1' DAYS))" +
          ") where rownum <=_TOPN_TOKEN \n";

  /**
   * 所有数据的 topN
   *
   * @param table
   * @param topn
   * @return
   */
  public static String topNNumLikeArticleSQL(String table, int topn) {
    return topN.replace(SourceAndSinkSql.TABLE_NAME_TOKEN, table)
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
