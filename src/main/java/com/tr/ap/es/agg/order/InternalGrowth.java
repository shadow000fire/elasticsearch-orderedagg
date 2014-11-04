package com.tr.ap.es.agg.order;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

public class InternalGrowth extends MetricsAggregation.SingleValue implements Growth
{

    public final static Type                      TYPE   = new Type("growth");

    private SearchHit[]                           hits;

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream()
                                                         {
                                                             @Override
                                                             public InternalGrowth readResult(StreamInput in)
                                                                     throws IOException
                                                             {
                                                                 InternalGrowth result = new InternalGrowth();
                                                                 result.readFrom(in);
                                                                 return result;
                                                             }
                                                         };

    public static void registerStreams()
    {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    InternalGrowth()
    {
    } // for serialization

    public InternalGrowth(String name, SearchHit[] hits)
    {
        super(name);
        this.hits = hits;
    }

    @Override
    public double value()
    {
        return hits == null ? 0 : hits.length;
    }

    @Override
    public Type type()
    {
        return TYPE;
    }

    @Override
    public InternalGrowth reduce(ReduceContext reduceContext)
    {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1)
        {
            return (InternalGrowth) aggregations.get(0);
        }
        else
        {
            // TODO or you could merge 2 ordered lists
            throw new RuntimeException("Should never have anyything to reduce");
        }
    }
    
    @Override
    public void readFrom(StreamInput in) throws IOException
    {
        name = in.readString();
        int len = in.readVInt();
        if (len == 0)
        {
            hits = InternalSearchHits.EMPTY;
        }
        else
        {
            hits = new SearchHit[len];
            for (int ii = 0; ii < len; ii++)
            {
                hits[ii] = InternalSearchHit.readSearchHit(
                        in,
                        InternalSearchHits.streamContext().streamShardTarget(
                                InternalSearchHits.StreamContext.ShardTargetType.STREAM));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException
    {
        out.writeString(name);
        if (hits == null || hits.length == 0)
        {
            out.writeVInt(0);
        }
        else
        {
            out.writeVInt(hits.length);
            for (SearchHit hit : hits)
            {
                hit.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startArray("hits");
        for(SearchHit hit:hits)
        {
            hit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public SearchHit[] getHits()
    {
        return hits;
    }

}
