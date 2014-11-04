package com.tr.ap.es.agg.order;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;

public interface Growth extends Aggregation
{
    SearchHit[] getHits();
}

