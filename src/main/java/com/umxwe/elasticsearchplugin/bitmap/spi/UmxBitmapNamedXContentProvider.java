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
 * 1、需要继承 NamedXContentProvider，来扩展es插件，才能在java api client中使用该聚合器，不然es查询返回了结果，但结果不能进行解析，找不到该聚合器；
 * 2、在resources目录中需要创建META-INF/services 目录，建立org.elasticsearch.plugins.spi.NamedXContentProvider文件，文件中写入UmxBitmapNamedXContentProvider的包路径
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/10
 */
public class UmxBitmapNamedXContentProvider implements NamedXContentProvider {
    /**
     * 实现getNamedXContentParsers方法，返回该聚合器相关的转换对象等内容
     *
     * @return
     */
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(BitmapAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedBitmap.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}