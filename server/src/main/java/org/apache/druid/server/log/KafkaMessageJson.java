package org.apache.druid.server.log;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * druid job json
 */
@Data
public class KafkaMessageJson {

  private String queryId;
  private String sqlQueryId;
  private String dataSource;
  private String queryType;
  private String isNested;
  private String hasFilters;
  private String remoteAddr;
  private String duration;
  private String descending;
  private DateTime timestamp;
  private QueryStats queryStats;
  private Query query;
  private ObjectMapper mapper;

  private static final Logger LOG = new Logger(KafkaMessageJson.class);


  public KafkaMessageJson(RequestLogLine requestLogLine, ObjectMapper map) {
    query = requestLogLine.getQuery();
    queryId = query.getId();
    sqlQueryId = StringUtils.nullToEmptyNonDruidDataString(query.getSqlQueryId());
    dataSource = findInnerDatasource(query).toString();
    queryType = query.getType();
    isNested = String.valueOf(!(query.getDataSource() instanceof TableDataSource));
    hasFilters = Boolean.toString(query.hasFilters());
    remoteAddr = requestLogLine.getRemoteAddr();
    duration = query.getDuration().toString();
    descending = Boolean.toString(query.isDescending());
    timestamp = requestLogLine.getTimestamp().plusHours(8);  //为解决druid时区与北京时间差8小时的问题
    queryStats = requestLogLine.getQueryStats();
    mapper = map;
  }

  @Override
  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put("queryId", queryId);
      jsonObject.put("sqlQueryId", sqlQueryId);
      jsonObject.put("queryType", queryType);
      jsonObject.put("dataSource", dataSource);
      jsonObject.put("isNested", isNested);
      jsonObject.put("hasFilters", hasFilters);
      jsonObject.put("remoteAddr", remoteAddr);
      jsonObject.put("duration", duration);
      jsonObject.put("descending", descending);
      jsonObject.put("timestamp", timestamp);

      String queryJson = mapper.writeValueAsString(query);
      jsonObject.put("queryJson", queryJson);

      Map<String, Object> stats = queryStats.getStats();
      jsonObject.put("queryTime", stats.get("query/time"));  //毫秒
      jsonObject.put("queryBytes", stats.get("query/bytes"));  //查询返回的量
      jsonObject.put("success", stats.get("success"));
      jsonObject.put("identity", stats.get("identity"));


      jsonObject.put("exception", stats.get("exception"));
      jsonObject.put("reason", stats.get("reason"));

      // 预留位
      jsonObject.put("user", stats.get("remoteUser"));
      jsonObject.put("userAgent", stats.get("other"));
      jsonObject.put("other", "0");

    } catch (Exception e) {
      LOG.error("Failed to parse RequestLogLine to a json object. " + e);
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
