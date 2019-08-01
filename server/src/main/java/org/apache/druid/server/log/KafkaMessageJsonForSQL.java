package org.apache.druid.server.log;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import lombok.Data;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.*;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * cerate by JSQ
 * druid job json for SQL query
 */
@Data
public class KafkaMessageJsonForSQL {
  private String dataSource;
  private String queryType;
  private String isNested;
  private String hasFilters;
  private String duration;
  private String descending;

  private String queryId;
  private String sqlQueryId;
  private String remoteAddr;
  private DateTime timestamp;
  private QueryStats queryStats;
  private Map<String, Object> sqlQueryContext;
  private String sql;



  private static final Logger LOG = new Logger(KafkaMessageJsonForSQL.class);


  public KafkaMessageJsonForSQL(RequestLogLine requestLogLine) {
    dataSource = "";
    queryType = "";
    isNested = "";
    hasFilters = "";
    duration = "";
    descending = "";
    remoteAddr = requestLogLine.getRemoteAddr();
    timestamp = requestLogLine.getTimestamp().plusHours(8);  //为解决druid时区与北京时间差8小时的问题
    queryStats = requestLogLine.getQueryStats();
    sql = requestLogLine.getSql();
    sqlQueryContext = requestLogLine.getSqlQueryContext();
    queryId = "";
    sqlQueryId = "";
  }

  @Override
  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("queryType", queryType);
      jsonObject.put("isNested", isNested);
      jsonObject.put("hasFilters", hasFilters);
      jsonObject.put("duration", duration);
      jsonObject.put("descending", descending);

      if (sqlQueryContext.containsKey("sqlQueryId")) {
        jsonObject.put("sqlQueryId", StringUtils.nullToEmptyNonDruidDataString(sqlQueryContext.get("sqlQueryId").toString()));
      } else {
        jsonObject.put("sqlQueryId", "null");
      }
      if (sqlQueryContext.containsKey("nativeQueryIds")) {
        jsonObject.put("queryId", StringUtils.nullToEmptyNonDruidDataString(sqlQueryContext.get("nativeQueryIds").toString()));
      } else {
        jsonObject.put("queryId", "null");
      }
      jsonObject.put("remoteAddr", remoteAddr);
      jsonObject.put("timestamp", timestamp);
      Map<String, Object> stats = queryStats.getStats();
      jsonObject.put("dataSource", stats.get("dataSource"));
      jsonObject.put("queryTime", stats.get("sqlQuery/time"));  //毫秒
      jsonObject.put("queryBytes", stats.get("sqlQuery/bytes"));  //查询返回的量
      jsonObject.put("queryJson", sql);
      jsonObject.put("success", stats.get("success"));
      jsonObject.put("identity", stats.get("identity"));


      jsonObject.put("exception", stats.get("exception"));
      jsonObject.put("reason", stats.get("reason"));

      // 预留位
      jsonObject.put("user", stats.get("remoteUser"));
      jsonObject.put("userAgent", stats.get("other"));
      jsonObject.put("other", "true");

    } catch (Exception e) {
      LOG.error("Failed to parse RequestLogLine to a sql json object. " + e);
    }
    return jsonObject.toString();
  }


  private Object findInnerDatasource(Query query)
  {
    DataSource _ds = query.getDataSource();
    if (_ds instanceof TableDataSource) {
      return ((TableDataSource) _ds).getName();
    }
    if (_ds instanceof QueryDataSource) {
      return findInnerDatasource(((QueryDataSource) _ds).getQuery());
    }
    if (_ds instanceof UnionDataSource) {
      return Joiner.on(",")
              .join(
                      ((UnionDataSource) _ds)
                              .getDataSources()
                              .stream()
                              .map(TableDataSource::getName)
                              .collect(Collectors.toList())
              );
    } else {
      // should not come here
      return query.getDataSource();
    }
  }

}
