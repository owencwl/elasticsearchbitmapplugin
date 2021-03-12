/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.umxwe.elasticsearchplugin.bitmap;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class BitmapAggregatorFactory extends ValuesSourceAggregatorFactory {

    public BitmapAggregatorFactory(String name, ValuesSourceConfig config, QueryShardContext context,
                                   AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
    }

    /**
     * 注册该聚合器，并且指定该聚合器能够处理的字段类型（bytes,numeric etc.）
     *
     * @param builder
     */
    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(BitmapAggregationBuilder.NAME,
                org.elasticsearch.common.collect.List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.NUMERIC),
                (MetricAggregatorSupplier) BitmapAggregator::new);
    }

    /**
     * 创建核心的聚合处理逻辑
     *
     * @param searchContext
     * @param parent
     * @param metadata
     * @return
     * @throws IOException
     */
    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new BitmapAggregator(name, config, searchContext, parent, metadata);

    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {

        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config,
                BitmapAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof MetricAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected MetricAggregatorSupplier, found [" +
                    aggregatorSupplier.getClass().toString() + "]");
        }
        return ((MetricAggregatorSupplier) aggregatorSupplier).build(name, config, searchContext, parent, metadata);
//        return new BitmapAggregator(name, config, searchContext, parent,metadata);
    }
}
