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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
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

    final ValuesSource.Bytes valuesSource;
    //    final ValuesSource.Bytes.WithOrdinals source;
    final DocValueFormat format;
    private Collector collector;

    private ObjectArray<Roaring64Bitmap> sums;
    private ObjectArray<FixedBitSet> visitedOrds;

    public BitmapAggregator(String name, ValuesSourceConfig valuesSourceConfig, SearchContext searchContext, Aggregator aggregator, Map<String, Object> stringObjectMap) throws IOException {
        super(name, searchContext, aggregator, stringObjectMap);
        this.format = valuesSourceConfig.format();
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Bytes) valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            sums = context.bigArrays().newObjectArray(1);
        }
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }


        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
            final SortedBinaryDocValues values = source.bytesValues(ctx);
            final SortedSetDocValues ordinalValues = source.globalOrdinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                return new EmptyCollector();
            }
            return new OrdinalsCollector(sums, ordinalValues, values, context.bigArrays());

//            final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
//            final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
//            // only use ordinals if they don't increase memory usage by more than 25%
//            if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
//                return new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
//            }

        }
        return new EmptyCollector();
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
//        postCollectLastCollector();

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

    @Override
    protected void doPostCollection() throws IOException {
//        postCollectLastCollector();
    }

    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect() throws IOException;

    }

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

    private static class OrdinalsCollector extends Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

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
//            visitedOrds = bigArrays.grow(visitedOrds, bucket + 1);
//            FixedBitSet bits = visitedOrds.get(bucket);
//            if (bits == null) {
//                bits = new FixedBitSet(maxOrd);
//                visitedOrds.set(bucket, bits);
//            }
//            if (values.advanceExact(doc)) {
//                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
//                    bits.set((int) ord);
//                }
//            }

//            if (values.advanceExact(doc)) {
//                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
//                    roaring64Bitmap.add(ord);
//                    roaring64Bitmap.runOptimize();
//                }
////                LOG.info("LeafBucketCollectorBase collect: doc [{}], bucket [{}], bitmap [{}]", doc, bucket, roaring64Bitmap);
//            }

//            sums = bigArrays.grow(sums, bucket + 1);
//            Roaring64Bitmap roaring64Bitmap = sums.get(bucket);
//            if (roaring64Bitmap == null) {
//                roaring64Bitmap = new Roaring64Bitmap();
//                sums.set(bucket, roaring64Bitmap);
//            }
//
//            if (binaryDocValues.advanceExact(doc)) {
//                final int valuesCount = binaryDocValues.docValueCount();
//                for (int i = 0; i < valuesCount; i++) {
//                    BytesRef valueAt = binaryDocValues.nextValue();
//                    roaring64Bitmap.add(Long.valueOf(BitmapUtil.stringToAscii(valueAt.utf8ToString())));
//                    roaring64Bitmap.runOptimize();
//                }
//            }

            if (binaryDocValues.advanceExact(doc)) {
                final int valuesCount = binaryDocValues.docValueCount();
                Roaring64Bitmap sum = new Roaring64Bitmap();
                for (int i = 0; i < valuesCount; i++) {
                    BytesRef valueAt = binaryDocValues.nextValue();
                    Roaring64Bitmap roaringBitmap = BitmapUtil.encodeBitmap(valueAt.utf8ToString());
                    sum.or(roaringBitmap);
                }
                Roaring64Bitmap roaringBitmap = sums.get(bucket);
                if (roaringBitmap == null) {
                    sums.set(bucket, sum);
                } else {
                    roaringBitmap.or(sum);
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
            final FixedBitSet allVisitedOrds = new FixedBitSet(maxOrd);
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                final FixedBitSet bits = visitedOrds.get(bucket);
                if (bits != null) {
                    allVisitedOrds.or(bits);
                }
            }
            LOG.info("allVisitedOrds:{}", allVisitedOrds.length());

//            final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
            try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                for (int ord = allVisitedOrds.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS;
                     ord = ord + 1 < maxOrd ? allVisitedOrds.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                    final BytesRef value = values.lookupOrd(ord);

//                    org.elasticsearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, Long.valueOf(BitmapUtil.stringToAscii(value.utf8ToString())));
                }

                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final FixedBitSet bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
                        for (int ord = bits.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS;
                             ord = ord + 1 < maxOrd ? bits.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                            roaring64Bitmap.add(hashes.get(ord));
                        }
                        sums.set(bucket, roaring64Bitmap);
                    }
                }
            }


            /**
             * 每个bucket进行或运算，获得不重复的值
             */
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
            Releasables.close(visitedOrds);
        }

    }


    //    @Override
//    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
//            final LeafBucketCollector sub) throws IOException {
//        if (valuesSource == null) {
//            return LeafBucketCollector.NO_OP_COLLECTOR;
//        }
//        final BigArrays bigArrays = context.bigArrays();
//        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
//        final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
//
//
//        return new LeafBucketCollectorBase(sub, ordinalValues) {
//            @Override
//            public void collect(int doc, long bucket) throws IOException {
//                sums = bigArrays.grow(sums, bucket + 1);
//                Roaring64Bitmap roaring64Bitmap = sums.get(bucket);
//                if (roaring64Bitmap == null) {
//                    roaring64Bitmap=new Roaring64Bitmap();
//                    sums.set(bucket, roaring64Bitmap);
//                }
//                if( ordinalValues.advanceExact(doc)) {
//                    for (long ord = ordinalValues.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = ordinalValues.nextOrd()) {
//                        LOG.info("ordinalValues:{}",ord);
//                        roaring64Bitmap.add(ord);
//                    }
//                    LOG.info("LeafBucketCollectorBase collect: doc [{}], bucket [{}], bitmap [{}]", doc, bucket, roaring64Bitmap);
//                }
//
//
////               if( values.advanceExact(doc)){
////                   final int valuesCount = values.docValueCount();
////                   Roaring64Bitmap sum = new Roaring64Bitmap();
////                   for (int i = 0; i < valuesCount; i++) {
////                       BytesRef valueAt = values.nextValue();
////                       byte[] bytes = valueAt.bytes;
////                       String tmp=new String(bytes, "utf-8");
////                       LOG.info("getLeafCollector:{}",tmp );
//////                       RoaringBitmap roaringBitmap = BitmapUtil.deserializeBitmap(bytes);
////                       Roaring64Bitmap roaringBitmap = BitmapUtil.encodeBitmap(tmp);
////                       sum.or(roaringBitmap);
////                   }
////                   Roaring64Bitmap roaringBitmap = sums.get(bucket);
////                   if (roaringBitmap == null) {
////                       sums.set(bucket, sum);
////                   } else {
////                       roaringBitmap.or(sum);
////                   }
////                   LOG.debug("LeafBucketCollectorBase collect: doc [{}], bucket [{}], bitmap [{}]", doc, bucket, sums.get(bucket));
////               }
//            }
//            public void postCollect() throws IOException {
//                /**
//                 * 每个bucket进行或运算，获得不重复的值
//                 */
//                Roaring64Bitmap roaring64Bitmap=new Roaring64Bitmap();
//                for (long bucket = sums.size() - 1; bucket >= 0; --bucket) {
//                    final Roaring64Bitmap bitmap = sums.get(bucket);
//                    if (bitmap != null) {
//                        roaring64Bitmap.or(bitmap);
//                    }
//                }
//
//            }
//
//        };
//    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
//        LOG.debug("BitmapAggregator buildAggregation: bucket [{}], sum [{}]", bucket, this.sums);
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalBitmap(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBitmap(name, new Roaring64Bitmap(), format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums);
    }

}
