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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BitmapAggregator extends MetricsAggregator {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregator.class);

    final ValuesSource valuesSource;
    final DocValueFormat format;
    private Collector collector;
    private ObjectArray<Roaring64Bitmap> sums;//用来收集每个bucket中的bitmap

    public BitmapAggregator(String name, ValuesSourceConfig valuesSourceConfig, SearchContext searchContext, Aggregator aggregator, Map<String, Object> stringObjectMap) throws IOException {
        super(name, searchContext, aggregator, stringObjectMap);
        this.format = valuesSourceConfig.format();
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            sums = context.bigArrays().newObjectArray(1);
        }
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }
        /**
         * string类型的字段
         */
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
            final SortedBinaryDocValues values = source.bytesValues(ctx);
            final SortedSetDocValues ordinalValues = source.globalOrdinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                return new EmptyCollector();
            }
            return new OrdinalsCollector(sums, ordinalValues, values, context.bigArrays());
        } else if (valuesSource instanceof ValuesSource.Numeric) {
            /**
             * long或者double类型的字段
             */
            ValuesSource.Numeric numeric = (ValuesSource.Numeric) valuesSource;
            SortedNumericDocValues sortedNumericDocValues = numeric.longValues(ctx);
            return new NumericCollector(sums, sortedNumericDocValues, context.bigArrays());
        }
        return new EmptyCollector();
    }

    /**
     * 创建 collector，用来收集所有数据，这里会循环被调用
     *
     * @param ctx
     * @param sub
     * @return
     * @throws IOException
     */
    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() throws IOException {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    /**
     * collector收集之后调用
     *
     * @throws IOException
     */
    @Override
    protected void doPostCollection() throws IOException {
//        postCollectLastCollector();
    }

    /**
     * Collector内部类
     */
    private abstract static class Collector extends LeafBucketCollector implements Releasable {
        public abstract void postCollect() throws IOException;
    }

    /**
     * 创建空的Collector
     */
    private static class EmptyCollector extends Collector {
        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * 创建针对数字类型的Collector
     */
    private class NumericCollector extends Collector {
        private final BigArrays bigArrays;
        private ObjectArray<Roaring64Bitmap> sums;
        private SortedNumericDocValues sortedNumericDocValues;

        public NumericCollector(ObjectArray<Roaring64Bitmap> sums, SortedNumericDocValues sortedNumericDocValues, BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.sums = sums;
            this.sortedNumericDocValues = sortedNumericDocValues;
        }

        @Override
        public void postCollect() throws IOException {

        }

        @Override
        public void close() {

        }

        /**
         * 核心方法，收集每条数据用作计算
         *
         * @param doc
         * @param bucket
         * @throws IOException
         */
        @Override
        public void collect(int doc, long bucket) throws IOException {
            sums = bigArrays.grow(sums, bucket + 1);
            Roaring64Bitmap roaring64Bitmap = sums.get(bucket);
            if (roaring64Bitmap == null) {
                roaring64Bitmap = new Roaring64Bitmap();
                sums.set(bucket, roaring64Bitmap);
            }
            if (sortedNumericDocValues.advanceExact(doc)) {
                final int valuesCount = sortedNumericDocValues.docValueCount();
                for (int i = 0; i < valuesCount; i++) {
                    long valueAt = sortedNumericDocValues.nextValue();
                    roaring64Bitmap.add(valueAt);
//                    roaring64Bitmap.runOptimize();
                }
            }
        }
    }

    private static class OrdinalsCollector extends Collector {

        private final BigArrays bigArrays;
        private final SortedSetDocValues values;
        private final SortedBinaryDocValues binaryDocValues;
        private final int maxOrd;
        private ObjectArray<FixedBitSet> visitedOrds;
        private ObjectArray<Roaring64Bitmap> sums;


        OrdinalsCollector(ObjectArray<Roaring64Bitmap> sums, SortedSetDocValues values, SortedBinaryDocValues binaryDocValues,
                          BigArrays bigArrays) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            maxOrd = (int) values.getValueCount();
            this.bigArrays = bigArrays;
            this.values = values;
            this.binaryDocValues = binaryDocValues;
            this.sums = sums;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            sums = bigArrays.grow(sums, bucket + 1);
            Roaring64Bitmap roaring64Bitmap = sums.get(bucket);
            if (roaring64Bitmap == null) {
                roaring64Bitmap = new Roaring64Bitmap();
                sums.set(bucket, roaring64Bitmap);
            }

            if (binaryDocValues.advanceExact(doc)) {
                final int valuesCount = binaryDocValues.docValueCount();
                for (int i = 0; i < valuesCount; i++) {
                    BytesRef valueAt = binaryDocValues.nextValue();
                    /**
                     * 车牌号 转换为 long类型，在500w数据下，有性能问题，较为耗时
                     */
                    roaring64Bitmap.add(Long.valueOf(BitmapUtil.stringToAscii(valueAt.utf8ToString())));
//                    roaring64Bitmap.runOptimize();
                }
            }
        }

        /**
         * 此方法只会执行最后一次
         *
         * @throws IOException
         */
        @Override
        public void postCollect() throws IOException {
//            final FixedBitSet allVisitedOrds = new FixedBitSet(maxOrd);
//            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
//                final FixedBitSet bits = visitedOrds.get(bucket);
//                if (bits != null) {
//                    allVisitedOrds.or(bits);
//                }
//            }
//            /**
//             * 每个bucket进行或运算，获得不重复的值
//             */
//            Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
//            for (long bucket = sums.size() - 1; bucket >= 0; --bucket) {
//                final Roaring64Bitmap bitmap = sums.get(bucket);
//                if (bitmap != null) {
//                    roaring64Bitmap.or(bitmap);
//                }
//            }
//            LOG.info("allVisitedOrds:{},roaring64Bitmap:{} ", allVisitedOrds.length(), roaring64Bitmap.getLongCardinality());
        }

        @Override
        public void close() {
//            Releasables.close(visitedOrds);
        }

    }

    /**
     * 每个bucket内生产一个InternalBitmap，用来做reduce计算
     *
     * @param bucket
     * @return
     */
    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalBitmap(name, sums.get(bucket), format, metadata());
    }

    /**
     * 生产一个空的InternalBitmap
     *
     * @return
     */
    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBitmap(name, new Roaring64Bitmap(), format, metadata());
    }

    /**
     * 释放资源
     */
    @Override
    public void doClose() {
        Releasables.close(sums);
    }


    public void query() throws IOException {
        BooleanQuery newQuery = new BooleanQuery.Builder()
                .add(context.query(), BooleanClause.Occur.MUST)
//                .add(new SearchAfterSortedDocQuery(applySortFieldRounding(indexSortPrefix), fieldDoc), BooleanClause.Occur.FILTER)
                .build();
        Weight weight = context.searcher().createWeight(context.searcher().rewrite(newQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
//        context.searcher().search();

    }
}
