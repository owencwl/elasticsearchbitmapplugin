package com.umxwe.elasticsearchplugin.bitmap;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

/**
 * 解析结果逻辑
 */
public class ParsedBitmap extends ParsedAggregation implements BitmapDistinct {
    protected long cardinality;
    protected byte[] bitmapByte;

    @Override
    public byte[] getBitmapByte() {
        return bitmapByte;
    }

    @Override
    public long getDistinctValue() {
        return cardinality;
    }

    @Override
    public String getType() {
        return BitmapAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), bitmapByte);
        builder.field("Cardinality", cardinality);
        return builder;
    }

    private static final ObjectParser<ParsedBitmap, Void> PARSER = new ObjectParser<>(ParsedBitmap.class.getSimpleName(),
            true, ParsedBitmap::new);

    static {
        declareStatsFields(PARSER);
    }

    /**
     * 结果解析，要与InternalBitmap.doXContentBody 中的返回字段名称一直
     *
     * @param objectParser
     */
    private static void declareStatsFields(ObjectParser<ParsedBitmap, Void> objectParser) {
        declareAggregationFields(objectParser);
        PARSER.declareLong((agg, value) -> agg.cardinality = value, new ParseField("Cardinality"));
        PARSER.declareField((agg, value) -> agg.bitmapByte = value, p -> p.binaryValue(), CommonFields.VALUE, ObjectParser.ValueType.VALUE);
    }

    public static ParsedBitmap fromXContent(XContentParser parser, final String name) {
        ParsedBitmap sum = PARSER.apply(parser, null);
        sum.setName(name);
        return sum;
    }
}
