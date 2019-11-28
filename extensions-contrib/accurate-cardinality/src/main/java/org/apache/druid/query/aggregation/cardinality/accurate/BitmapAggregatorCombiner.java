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

import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class BitmapAggregatorCombiner extends ObjectAggregateCombiner<LongBitmapCollector>
{

  @Nullable
  private LongBitmapCollector collector;
  private LongBitmapCollectorFactory collectorFactory;

  public BitmapAggregatorCombiner(
      LongBitmapCollectorFactory collectorFactory
  )
  {
    this.collectorFactory = collectorFactory;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    collector = null;
    fold(selector);
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (collector == null) {
      collector = collectorFactory.makeEmptyCollector();
    }
    collector.fold((LongBitmapCollector) object);
  }

  @Nullable
  @Override
  public LongBitmapCollector getObject()
  {
    return collector;
  }

  @Override
  public Class<LongRoaringBitmapCollector> classOfObject()
  {
    return LongRoaringBitmapCollector.class;
  }
}
