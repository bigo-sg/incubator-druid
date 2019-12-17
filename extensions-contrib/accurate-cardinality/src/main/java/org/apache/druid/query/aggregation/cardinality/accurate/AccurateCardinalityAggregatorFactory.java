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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;


public class AccurateCardinalityAggregatorFactory extends AggregatorFactory
{
  private static final LongBitmapCollectorFactory DEFAULT_BITMAP_FACTORY = new LongRoaringBitmapCollectorFactory();
  private static final String DEFAULT_OPENONEID = "false";

  private final String name;
  private final DimensionSpec field;
  private final LongBitmapCollectorFactory longBitmapCollectorFactory;
  private final String nameSpace;
  private final String openOneId;

  @JsonCreator
  public AccurateCardinalityAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("field") final DimensionSpec field,
      @Nullable @JsonProperty("nameSpace") String nameSpace,
      @Nullable @JsonProperty("openOneId") String openOneId,
      @JsonProperty("longBitmapCollectorFactory") LongBitmapCollectorFactory longBitmapCollectorFactory
  )
  {
    this.name = name;
    this.field = field;
    this.nameSpace = Preconditions.checkNotNull(nameSpace, "nameSpace can not be null");
    this.openOneId = openOneId == null
                    ? DEFAULT_OPENONEID
                    : openOneId;
    this.longBitmapCollectorFactory = longBitmapCollectorFactory == null
                                      ? DEFAULT_BITMAP_FACTORY
                                      : longBitmapCollectorFactory;
    VariableConfig.setNameSpace(this.nameSpace);
    VariableConfig.setOpenOneId(this.openOneId);
  }

  public AccurateCardinalityAggregatorFactory(
      String name,
      DimensionSpec field,
      String nameSpace,
      String openOneId
  )
  {
    this(name, field, nameSpace, openOneId,  DEFAULT_BITMAP_FACTORY);
  }

  public AccurateCardinalityAggregatorFactory(
          String name,
          DimensionSpec field
  )
  {
    this(name, field, "", null,  DEFAULT_BITMAP_FACTORY);
  }

  public AccurateCardinalityAggregatorFactory(
          String name,
          String field
  )
  {
    this(name, field, "", null,  DEFAULT_BITMAP_FACTORY);
  }

  public AccurateCardinalityAggregatorFactory(
      String name,
      String field,
      String nameSpace,
      String openOneId
  )
  {
    this(name, field, nameSpace, openOneId, DEFAULT_BITMAP_FACTORY);
  }


  public AccurateCardinalityAggregatorFactory(
      String name,
      String field,
      String nameSpace,
      String openOneId,
      LongBitmapCollectorFactory longBitmapCollectorFactory
  )
  {
    this(name, new DefaultDimensionSpec(field, field, ValueType.LONG), nameSpace, openOneId, longBitmapCollectorFactory);
  }

  @JsonProperty
  public DimensionSpec getField()
  {
    return field;
  }

  @JsonProperty
  public String getNameSpace()
  {
    return nameSpace;
  }

  @JsonProperty
  public String getOpenOneId()
  {
    return openOneId;
  }

  @JsonProperty
  public LongBitmapCollectorFactory getLongBitmapCollectorFactory()
  {
    return longBitmapCollectorFactory;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    return factorizeInternal(columnFactory, true);
  }

  private BaseAccurateCardinalityAggregator factorizeInternal(ColumnSelectorFactory columnFactory, boolean onHeap)
  {
    if (field == null || field.getDimension() == null) {
      return new NoopAccurateCardinalityAggregator(longBitmapCollectorFactory, onHeap);
    }
      ColumnValueSelector columnValueSelector = columnFactory.makeColumnValueSelector(field.getDimension());
      if (columnValueSelector instanceof NilColumnValueSelector) {
        return new NoopAccurateCardinalityAggregator(longBitmapCollectorFactory, onHeap);
      }
      return new ObjectAccurateCardinalityAggregator(
          columnFactory.makeColumnValueSelector(field.getDimension()),
          longBitmapCollectorFactory,
          onHeap
      );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    return factorizeInternal(columnFactory, false);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<LongBitmapCollector>()
    {
      @Override
      public int compare(LongBitmapCollector c1, LongBitmapCollector c2)
      {
        return c1.compareTo(c2);
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    LongBitmapCollector lhsLongBitmapCollector = (LongBitmapCollector) lhs;
    return lhsLongBitmapCollector.fold((LongBitmapCollector) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BitmapAggregatorFactory(name, name);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new BitmapAggregatorCombiner(longBitmapCollectorFactory);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Lists.transform(
        ImmutableList.of(field),
        new Function<DimensionSpec, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(DimensionSpec input)
          {
            return new AccurateCardinalityAggregatorFactory(input.getOutputName(), input, nameSpace, openOneId, longBitmapCollectorFactory);
          }
        }
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
    return longBitmapCollectorFactory.makeCollector(buffer);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object == null) {
      return 0;
    }
    return ((LongBitmapCollector) object).getCardinality();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(field.getOutputName());
  }

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    /* LongAccurateCardinalityAggregator and BitmapAggregator actrually use onheap LongBitmapCollector to collect long-type dimension value.
       It just use the buffer an offset in buffer to locate according LongBitmapCollector, but not store data in it.
       So here just return 1.
    */
    return 1;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimSpecKey = field.getCacheKey();
    ByteBuffer retBuf = ByteBuffer.allocate(2 + dimSpecKey.length);
    retBuf.put(AggregatorUtil.ACCURATE_CARDINALITY_CACHE_TYPE_ID);
    retBuf.put(dimSpecKey);

    return retBuf.array();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccurateCardinalityAggregatorFactory factory = (AccurateCardinalityAggregatorFactory) o;
    return Objects.equal(name, factory.name) &&
           Objects.equal(field, factory.field) &&
            Objects.equal(nameSpace, factory.nameSpace) &&
            Objects.equal(openOneId, factory.openOneId) &&
           Objects.equal(longBitmapCollectorFactory, factory.longBitmapCollectorFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(name, field, nameSpace, openOneId, longBitmapCollectorFactory);
  }
}
