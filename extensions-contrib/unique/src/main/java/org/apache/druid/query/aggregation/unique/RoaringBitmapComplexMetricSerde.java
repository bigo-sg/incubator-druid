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

package org.apache.druid.query.aggregation.unique;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

// SerDe说明hive如何去处理一条记录，包括Serialize/Deserilize两个功能，
// Serialize把hive使用的java object转换成能写入hdfs的字节序列，或者其他系统能识别的流文件。
// Deserilize把字符串或者二进制流转换成hive能识别的java object对象。
public class RoaringBitmapComplexMetricSerde extends ComplexMetricSerde   //Serialize/Deserilize
{

  @Override
  public String getTypeName()
  {
    return "ImmutableRoaringBitmap";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<ImmutableRoaringBitmap> extractedClass()
      {
        return ImmutableRoaringBitmap.class;
      }

      @Nullable
      @Override
      public ImmutableRoaringBitmap extractValue(InputRow inputRow, String metricName)
      {
        final Object rawValue = inputRow.getRaw(metricName);
        if (rawValue == null) {
          return new MutableRoaringBitmap();
        } else if (rawValue instanceof ImmutableRoaringBitmap) {
          return (ImmutableRoaringBitmap) rawValue;
        } else {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return bitmap;
          }
          for (String dimensionValue : dimValues) {
            bitmap.add(Integer.parseInt(dimensionValue));
          }
          return bitmap;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed<ImmutableRoaringBitmap> ge = GenericIndexed.read(
        buffer,
        ImmutableRoaringBitmapObjectStrategy.STRATEGY,
        builder.getFileMapper()
    );
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return ImmutableRoaringBitmapObjectStrategy.STRATEGY;
  }


  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
