package com.tr.ap.es.agg.order;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

public class GrowthBuilder extends ValuesSourceMetricsAggregationBuilder<GrowthBuilder> {

    public GrowthBuilder(String name) {
        super(name, InternalGrowth.TYPE.name());
    }
}