package com.umxwe.elasticsearchplugin.bitmap;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class BitmapAggregationPlugin extends Plugin implements SearchPlugin {

    /**
     * es服务器中的插件中心，需要实现该plugin接口，服务器才能识别到；
     * 在resources中，创建plugin-descriptor.properties，对应修改其中的内容即可，本properties在es7.9.1中可以使用，其他版本有可能不同；
     *
     * @return
     */
    @Override
    public List<AggregationSpec> getAggregations() {
        return Collections.singletonList(new AggregationSpec(BitmapAggregationBuilder.NAME, BitmapAggregationBuilder::new
                , BitmapAggregationBuilder.PARSER).setAggregatorRegistrar(BitmapAggregationBuilder::registerAggregators)
                .addResultReader(InternalBitmap::new));
    }
}
