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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalBitmap extends InternalNumericMetricsAggregation.SingleValue implements BitmapDistinct {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregator.class);

    private final Roaring64Bitmap sum;

    InternalBitmap(String name, Roaring64Bitmap sum, DocValueFormat formatter,
                   Map<String, Object> metaData) {
        super(name, metaData);
        this.sum = sum;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalBitmap(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        byte[] bytes = in.readByteArray();
        this.sum = BitmapUtil.deserializeBitmap(bytes);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeByteArray(Objects.requireNonNull(BitmapUtil.serializeBitmap(sum)));
    }

    @Override
    public String getWriteableName() {
        return BitmapAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return sum == null ? 0 : sum.getLongCardinality();
    }

    @Override
    public long getDistinctValue() {
        return sum.getLongCardinality();
    }

    @Override
    public byte[] getBitmapByte() {
        return sum == null ? null : BitmapUtil.serializeBitmap(sum);
    }

    /**
     * reduce的过程，将每个聚合器中的bitmap进行汇聚
     *
     * @param aggregations
     * @param reduceContext
     * @return
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Roaring64Bitmap sum = new Roaring64Bitmap();
        for (InternalAggregation aggregation : aggregations) {
            sum.or(((InternalBitmap) aggregation).sum);
        }
        return new InternalBitmap(name, sum, format, getMetadata());
    }

    /**
     * 将计算结果返回出去
     *
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        LOG.info("InternalBitmap doXContentBody: Cardinalit_size: [{}]", sum.getLongCardinality());
        builder.field(CommonFields.VALUE.getPreferredName(), BitmapUtil.serializeBitmap(sum));
        builder.field("Cardinality", sum.getLongCardinality());
        return builder;
    }

}
