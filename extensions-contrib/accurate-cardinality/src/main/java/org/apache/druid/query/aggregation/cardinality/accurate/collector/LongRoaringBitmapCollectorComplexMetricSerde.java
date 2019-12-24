/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.cardinality.accurate.collector;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Ordering;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.query.aggregation.cardinality.accurate.AccurateCardinalityModule;
import org.apache.druid.query.aggregation.cardinality.accurate.VariableConfig;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.AutoRetryHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public class LongRoaringBitmapCollectorComplexMetricSerde extends ComplexMetricSerde
{
  private final static Logger LOG = LoggerFactory.getLogger(LongRoaringBitmapCollectorComplexMetricSerde.class);
  private LongBitmapCollectorFactory longBitmapCollectorFactory;
  private static Cache<String, String> cache =
          CacheBuilder.newBuilder().initialCapacity(15000).maximumSize(25000).build();
  private static DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
  private static AutoRetryHttpClient httpClient;
  static {
    defaultHttpClient.setHttpRequestRetryHandler(
            new HttpRequestRetryHandler() {
              @Override
              public boolean retryRequest(IOException exception, int executionCount, HttpContext httpContext) {
                if (executionCount > 3){
                  return false;
                }
                // unknown host
                if (exception instanceof UnknownHostException) {
                  return false;
                }
                // SSL handshake exception
                if (exception instanceof SSLException) {
                  return false;
                }
                // timeout or no response
                if (exception instanceof InterruptedIOException
                        || exception instanceof NoHttpResponseException) {
                  LOG.warn("Retry request, exception: " + exception + ", executionCount: " + executionCount);
                  return true;
                }
                // idempotent
                HttpRequest request = (HttpRequest)httpContext.getAttribute("http.request");
                if (!(request instanceof HttpEntityEnclosingRequest)) {
                  LOG.warn("Retry request, exception: " + exception + ", executionCount: " + executionCount);
                  return true;
                }
                return false;
              }
            });

    httpClient = new AutoRetryHttpClient(defaultHttpClient, new ServiceUnavailableRetryStrategy() {
      @Override
      public boolean retryRequest(HttpResponse httpResponse, int executeCount, HttpContext httpContext) {
        if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK && executeCount <= 3) {
          LOG.warn("Retry request, statusCode: " + httpResponse.getStatusLine().getStatusCode() + ", executeCount: " + executeCount);
          return true;
        }
        return false;
      }

      @Override
      public long getRetryInterval() {
        return 1000;
      }
    });

    httpClient.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 15000);
    httpClient.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 60000);
  }

  private static final ExtensionsConfig EXTENSIONS_CONFIG;
  static final Injector INJECTOR = GuiceInjectors.makeStartupInjector();
  static {
    EXTENSIONS_CONFIG = INJECTOR.getInstance(ExtensionsConfig.class);
  }

  public LongRoaringBitmapCollectorComplexMetricSerde(LongBitmapCollectorFactory longBitmapCollectorFactory)
  {
    this.longBitmapCollectorFactory = longBitmapCollectorFactory;
  }

  private static Ordering<LongBitmapCollector> comparator = new Ordering<LongBitmapCollector>()
  {
    @Override
    public int compare(
        LongBitmapCollector arg1,
        LongBitmapCollector arg2
    )
    {
      return arg1.toByteBuffer().compareTo(arg2.toByteBuffer());
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<LongRoaringBitmapCollector> extractedClass()
      {
        return LongRoaringBitmapCollector.class;
      }

      @Override
      public LongRoaringBitmapCollector extractValue(InputRow inputRow, String metricName)
      {
        Object object = inputRow.getRaw(metricName);
        if (object instanceof LongRoaringBitmapCollector) {
          return (LongRoaringBitmapCollector) object;
        }
        if (object == null || object == "") { object = 100000000; }
        final String objectStr = object.toString();
        LongRoaringBitmapCollector collector = (LongRoaringBitmapCollector) longBitmapCollectorFactory.makeEmptyCollector();

        if (VariableConfig.getOpenOneId(metricName).equals("false")) {
          collector.add(Long.parseLong(objectStr));
        } else {
          Long resuiltId;
          try {
            resuiltId = Long.valueOf(cache.get(objectStr + VariableConfig.getNameSpace(metricName), new Callable<String>() {
              @Override
              public String call() {
                return getId(VariableConfig.getNameSpace(metricName), objectStr + VariableConfig.getNameSpace(metricName));
              }
            }));
          } catch (Exception e) {
            throw new RuntimeException("Fetch id failed!", e);
          }
          collector.add(resuiltId);
        }
        return collector;
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), builder.getFileMapper());
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<LongRoaringBitmapCollector>()
    {

      @Override
      public int compare(
          LongRoaringBitmapCollector o1,
          LongRoaringBitmapCollector o2
      )
      {
        return comparator.compare(o1, o2);
      }

      @Override
      public Class<? extends LongRoaringBitmapCollector> getClazz()
      {
        return LongRoaringBitmapCollector.class;
      }

      @Override
      public LongRoaringBitmapCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        byte[] bytes = new byte[readOnlyBuffer.remaining()];
        readOnlyBuffer.get(bytes, 0, numBytes);

        return LongRoaringBitmapCollector.deserialize(bytes);
      }

      @Override
      public byte[] toBytes(LongRoaringBitmapCollector collector)
      {
        if (collector == null) {
          return new byte[]{};
        }
        ByteBuffer val = collector.toByteBuffer();
        byte[] retVal = new byte[val.remaining()];
        val.asReadOnlyBuffer().get(retVal);
        return retVal;
      }
    };
  }

  private String getId(String namespace, String key) {
    HashMap<String, String> params = new HashMap<>();
    params.put("namespace", namespace);
    params.put("key", key);

    JsonNode result = doGet(EXTENSIONS_CONFIG.getOneIdUrl(), params, "UTF-8");
    String id = result.get("id") == null? "0": result.get("id").asText();
    if (id.equals("0")) { LOG.error(" Highly likely get Id failed from oneId server! id = '0' "); }
    return id;
  }

  private JsonNode doGet(String url, Map<String, String> params, String charset) {
    if (StringUtils.isBlank(url)) {
      return null;
    }

    try {
      if (params != null && !params.isEmpty()) {
        List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
        for (Map.Entry<String, String> entry : params.entrySet()) {
          String value = entry.getValue();
          if (value != null) {
            pairs.add(new BasicNameValuePair(entry.getKey(), value));
          }
        }
        url += "?" + EntityUtils.toString(new UrlEncodedFormEntity(pairs, charset));
      }
      HttpGet httpGet = new HttpGet(url);
      HttpResponse response = httpClient.execute(httpGet);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        httpGet.abort();
        throw new RuntimeException("Unable to fetch id from oneid server, error status code: " + statusCode);
      }
      HttpEntity entity = response.getEntity();
      ObjectMapper result = new ObjectMapper();
      result.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
      result.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
      JsonNode document = null;
      if (entity != null) {
        document = result.readValue(EntityUtils.toString(entity, "utf-8"), JsonNode.class);
      }
      EntityUtils.consume(entity);
      return document;
    } catch (Exception e) {
      throw new RuntimeException("Unable to get id from oneId server!", e);
    }
  }

  public static boolean isInteger(String str) {
    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
    return pattern.matcher(str).matches();
  }
}
