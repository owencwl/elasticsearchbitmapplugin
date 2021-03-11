package com.umxwe.elasticsearchplugin.bitmap;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class BitmapAggregationPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return Collections.singletonList(new AggregationSpec(BitmapAggregationBuilder.NAME, BitmapAggregationBuilder::new
                , BitmapAggregationBuilder.PARSER).setAggregatorRegistrar(BitmapAggregationBuilder::registerAggregators)
                .addResultReader(InternalBitmap::new));
    }
}
