package org.apache.druid.query.aggregation.cardinality.accurate;

/**
 * @author jiangshequan
 * @title: VariableConfig
 * @date 2019/12/12 12:40
 */
public class VariableConfig {
  public static String nameSpace;
  public static String dataType;

  public static void setNameSpace(String name)
  {
    nameSpace = name;
  }
  public static String getNameSpace()
  {
    return nameSpace;
  }

  public static void setDataType(String type)
  {
    dataType = type;
  }
  public static String getDataType()
  {
    return dataType;
  }
}
