package org.apache.druid.query.aggregation.cardinality.accurate;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jiangshequan
 * @title: VariableConfig
 * @date 2019/12/12 12:40
 */
public class VariableConfig {
  public static Map<String, String> nameSpace;
  public static Map<String, String> openOneId;
  static {
    nameSpace = new ConcurrentHashMap<>();
    openOneId = new ConcurrentHashMap<>();
  }

  public static void setNameSpace(String columnName, String name)
  {
    nameSpace.put(columnName, name);
  }
  public static String getNameSpace(String columnName)
  {
    return nameSpace.get(columnName);
  }

  public static void setOpenOneId(String columnName, String open)
  {
    openOneId.put(columnName, open);
  }
  public static String getOpenOneId(String columnName)
  {
    return openOneId.get(columnName);
  }
}
