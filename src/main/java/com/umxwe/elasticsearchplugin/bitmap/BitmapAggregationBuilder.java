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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.*;

import java.io.IOException;
import java.util.Map;


public class BitmapAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Bytes, BitmapAggregationBuilder> {

    public static final String NAME = "umxbitmap";

    /**
     * 创建解析对象
     */
    public static final ObjectParser<BitmapAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, BitmapAggregationBuilder::new);

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
    }

    /**
     * 注册聚合器
     *
     * @param builder
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        BitmapAggregatorFactory.registerAggregators(builder);
    }

    /**
     * 构造函数
     *
     * @param bitmapAggregationBuilder
     * @param factoriesBuilder
     * @param metadata
     */
    public BitmapAggregationBuilder(BitmapAggregationBuilder bitmapAggregationBuilder, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(bitmapAggregationBuilder, factoriesBuilder, metadata);
    }

    public BitmapAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Read from a stream.
     */
    public BitmapAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * 需要返回AggregationBuilder
     *
     * @param factoriesBuilder
     * @param metadata
     * @return
     */
    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BitmapAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    /**
     * 指定默认的字段类型
     *
     * @return
     */
    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    /**
     * 返回自定义聚合工厂
     *
     * @param queryShardContext
     * @param config
     * @param parent
     * @param subFactoriesBuilder
     * @return
     * @throws IOException
     */
    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config, AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new BitmapAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }


    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    /**
     * 返回聚合器名称
     *
     * @return
     */
    @Override
    public String getType() {
        return NAME;
    }


}
