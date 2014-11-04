package com.tr.ap.es.agg.order;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;

public class GrowthPlugin extends AbstractPlugin
{
    public GrowthPlugin()
    {}

    public String name()
    {
        return "growth";
    }

    public String description()
    {
        return "Seqential docs for elastic";
    }

    public void onModule(AggregationModule module)
    {
        module.addAggregatorParser(GrowthParser.class);
        InternalGrowth.registerStreams();
    }

}