package com.umxwe.elasticsearchplugin.bitmap.spi;

import com.umxwe.elasticsearchplugin.bitmap.BitmapAggregationBuilder;
import com.umxwe.elasticsearchplugin.bitmap.ParsedBitmap;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * @ClassName UmxBitmapNamedXContentProvider
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/10
 */
public class UmxBitmapNamedXContentProvider implements NamedXContentProvider {
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(BitmapAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedBitmap.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}