package com.tr.ap.es.agg.order;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

public class GrowthParser implements Aggregator.Parser {

    protected final InternalAggregation.Type aggType;

    public GrowthParser() {
        this.aggType = InternalGrowth.TYPE;
    }

    @Override
    public String type() {
        return aggType.name();
    }

    protected boolean requiresSortedValues() {
        return false;
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> valueFieldSourceParser = ValuesSourceParser.numeric(aggregationName, aggType, context)
                .requiresSortedValues(requiresSortedValues()).scriptable(true)
                .build();
        ValuesSourceParser<ValuesSource.Numeric> orderFieldSourceParser = ValuesSourceParser.numeric(aggregationName, aggType, context)
                .requiresSortedValues(requiresSortedValues()).scriptable(true)
                .build();

        XContentParser.Token token;
        String currentFieldName = null;
        double min=Double.NEGATIVE_INFINITY, max=Double.POSITIVE_INFINITY;
        boolean minIsPercentage=false, maxIsPercentage=false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            else if(token==XContentParser.Token.VALUE_STRING)
            {
                if("min".equals(currentFieldName))
                {
                    String minStr=parser.text().trim();
                    if (minStr.charAt(minStr.length() - 1) == '%')
                    {
                        minIsPercentage = true;
                        min = Double.parseDouble(minStr.substring(0, minStr.length() - 1).trim());
                    }
                    else
                    {
                        min = Double.parseDouble(minStr);
                    }
                }
                else if("max".equals(currentFieldName))
                {
                    String maxStr=parser.text().trim();
                    if (maxStr.charAt(maxStr.length() - 1) == '%')
                    {
                        maxIsPercentage = true;
                        max = Double.parseDouble(maxStr.substring(0, maxStr.length() - 1).trim());
                    }
                    else
                    {
                        max = Double.parseDouble(maxStr);
                    }
                }
                else if("field".equals(currentFieldName))
                {
                    if (!valueFieldSourceParser.token("field", token, parser)) {
                        throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
                    }
                }
                else if("order_field".equals(currentFieldName))
                {
                    if (!orderFieldSourceParser.token("field", token, parser)) {
                        throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
                    }
                }
                else
                {
                    throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
                }
            }
            else if(token==XContentParser.Token.VALUE_NUMBER)
            {
                if("min".equals(currentFieldName))
                {
                    min=parser.doubleValue(true);
                }
                else if("max".equals(currentFieldName))
                {
                    max=parser.doubleValue(true);
                }
            }
        }
        return new GrowthAggregator.Factory(aggregationName, type(), valueFieldSourceParser.config(), orderFieldSourceParser.config(), min,
                 minIsPercentage,  max,  maxIsPercentage);
    }
}
