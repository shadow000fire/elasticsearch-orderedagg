package com.tr.ap.es.agg.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.hppc.DoubleArrayList;
import org.elasticsearch.common.hppc.IntArrayList;
import org.elasticsearch.common.hppc.LongArrayList;
import org.elasticsearch.common.hppc.ObjectArrayList;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

public class GrowthAggregator extends MetricsAggregator.SingleValue
{
    private final ValuesSource.Numeric   valueValuesSource;
    private final ValuesSource.Numeric   orderValuesSource;

    private LongValues                   comparableValues;
    private DoubleValues                 valueValues;

    private ObjectArray<LongArrayList>   comparables;
    private ObjectArray<DoubleArrayList> values;
    private ObjectArray<IntArrayList>   ids;

    private double                       min;
    private double                       max;
    private boolean                      minIsPercentage;
    private boolean                      maxIsPercentage;
    
    private final FieldsVisitor fieldsVisitor;
    private List<String> extractFieldNames;

    public GrowthAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valueValuesSource,
            ValuesSource.Numeric orderValuesSource, AggregationContext context, Aggregator parent, double min,
            boolean minIsPercentage, double max, boolean maxIsPercentage)
    {
        super(name, estimatedBucketsCount, context, parent);
        this.valueValuesSource = valueValuesSource;
        this.orderValuesSource = orderValuesSource;
        this.min = minIsPercentage? min/100.0 : min;
        this.minIsPercentage = minIsPercentage;
        this.max = maxIsPercentage ? max/100.0 : max;
        this.maxIsPercentage = maxIsPercentage;
        if (valueValuesSource != null)
        {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            comparables = bigArrays.newObjectArray(initialSize);
            values = bigArrays.newObjectArray(initialSize);
            ids = bigArrays.newObjectArray(initialSize);
        }
        
        //Get field loaders
        Set<String> fieldNames = new HashSet<>();
        for (String fieldName : context.searchContext().fieldNames()) {
            FieldMappers x = context.searchContext().smartNameFieldMappers(fieldName);
            if (x == null) {
                // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                if (context.searchContext().smartNameObjectMapper(fieldName) != null) {
                    throw new ElasticsearchIllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                }
            } else if (x.mapper().fieldType().stored()) {
                fieldNames.add(x.mapper().names().indexName());
            } else {
                if (extractFieldNames == null) {
                    extractFieldNames = new ArrayList<String>(4);
                }
                extractFieldNames.add(fieldName);
            }
        }
        fieldsVisitor = new CustomFieldsVisitor(fieldNames, extractFieldNames!=null);
        
        System.err.println("max="+this.max+", maxIsPercentage="+this.maxIsPercentage+", min="+this.min+", minIsPercentage="+this.minIsPercentage);
    }

    @Override
    public boolean shouldCollect()
    {
        return valueValuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader)
    {
        comparableValues = orderValuesSource.longValues();
        valueValues = valueValuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException
    {
        comparables = bigArrays.grow(comparables, owningBucketOrdinal + 1);
        values = bigArrays.grow(values, owningBucketOrdinal + 1);
        ids = bigArrays.grow(ids, owningBucketOrdinal + 1);

        LongArrayList tempLong = null;
        DoubleArrayList tempDouble = null;
        IntArrayList tempInt = null;
        double d = 0.0;
        long l = 0l;
        if (comparableValues.setDocument(doc) > 0 && valueValues.setDocument(doc) > 0)
        {
            tempLong = comparables.get(owningBucketOrdinal);
            if (tempLong == null)
            {
                tempLong = LongArrayList.newInstance();
                comparables.set(owningBucketOrdinal, tempLong);
            }
            l = comparableValues.nextValue();
            tempLong.add(l);

            tempDouble = values.get(owningBucketOrdinal);
            if (tempDouble == null)
            {
                tempDouble = DoubleArrayList.newInstance();
                values.set(owningBucketOrdinal, tempDouble);
            }
            d = valueValues.nextValue();
            tempDouble.add(d);
            
            tempInt = ids.get(owningBucketOrdinal);
            if (tempInt == null)
            {
                tempInt = IntArrayList.newInstance();
                ids.set(owningBucketOrdinal, tempInt);
            }
            tempInt.add(doc);
        }

    }

    @Override
    public double metric(long owningBucketOrd)
    {
        return Double.NaN;// We don't need this, is only to support ordering on
                          // sub-metrics
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal)
    {
        if (valueValuesSource == null || owningBucketOrdinal >= comparables.size())
        {
            return new InternalGrowth(name, InternalSearchHits.EMPTY);
        }

        LongArrayList compList = comparables.get(owningBucketOrdinal);
        DoubleArrayList valueList = values.get(owningBucketOrdinal);

        if (compList.size() < 2)
        {
            return new InternalGrowth(name, InternalSearchHits.EMPTY);
        }
        

        ObjectArrayList<SearchHit> hitList=new ObjectArrayList<SearchHit>(valueList.size());
        ParallelCollectionUtilities.sort(compList, valueList, ids.get(owningBucketOrdinal));
        final double[] valueArray=valueList.buffer;
        double prev = valueArray[0], val = 0.0, growth = 0.0, percent=0.0;
        int docId=-1;
        Map<String, SearchHitField> searchFields;
        List<Object> growthFieldValue;
        List<Object> growthPercentFieldValue;
        
        for(int index=1;index<valueArray.length;index++)
        {
            val = valueArray[index];
            growth = val - prev;
            percent=prev==0.0 ? Double.POSITIVE_INFINITY : growth/prev;
            prev = val;
            if(minIsPercentage )
            {
               if(percent<min)
               {
                   continue;
               }
            }
            else
            {
                if(growth<min)
                {
                    continue;
                } 
            }
            if(maxIsPercentage )
            {
               if(percent>max)
               {
                   continue;
               }
            }
            else
            {
                if(growth>max)
                {
                    continue;
                } 
            }
            
            
            //we passed the filter if we get here
            docId=ids.get(owningBucketOrdinal).get(index);
//            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
            loadStoredFields(context.searchContext(), fieldsVisitor, docId);
            fieldsVisitor.postProcess(context.searchContext().mapperService());

            searchFields = new HashMap<>(fieldsVisitor.fields().size());
            if (!fieldsVisitor.fields().isEmpty()) {
                for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                    searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
                }
            }
            growthFieldValue=new ArrayList<Object>(1);
            growthFieldValue.add(growth);
            searchFields.put("growth", new InternalSearchHitField("growth", growthFieldValue));
            growthPercentFieldValue=new ArrayList<Object>(1);
            growthPercentFieldValue.add(percent);
            searchFields.put("growth_percentage", new InternalSearchHitField("growth_percentage", growthPercentFieldValue));

            DocumentMapper documentMapper = context.searchContext().mapperService().documentMapper(fieldsVisitor.uid().type());
            Text typeText;
            if (documentMapper == null) {
                typeText = new StringAndBytesText(fieldsVisitor.uid().type());
            } else {
                typeText = documentMapper.typeText();
            }
            InternalSearchHit searchHit = new InternalSearchHit(docId, fieldsVisitor.uid().id(), typeText, searchFields);
            searchHit.shardTarget(context.searchContext().shardTarget());
            hitList.add(searchHit);

            // go over and extract fields that are not mapped / stored
            if (extractFieldNames != null) {
                int readerIndex = ReaderUtil.subIndex(docId, context.searchContext().searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = context.searchContext().searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = docId - subReaderContext.docBase;
                
                context.searchContext().lookup().setNextReader(subReaderContext);
                context.searchContext().lookup().setNextDocId(subDoc);
                if (fieldsVisitor.source() != null) {
                    context.searchContext().lookup().source().setNextSource(fieldsVisitor.source());
                }
                
                for (String extractFieldName : extractFieldNames) {
                    List<Object> values = context.searchContext().lookup().source().extractRawValues(extractFieldName);
                    if (!values.isEmpty()) {
                        if (searchHit.fieldsOrNull() == null) {
                            searchHit.fields(new HashMap<String, SearchHitField>(2));
                        }

                        SearchHitField hitField = searchHit.fields().get(extractFieldName);
                        if (hitField == null) {
                            hitField = new InternalSearchHitField(extractFieldName, new ArrayList<>(2));
                            searchHit.fields().put(extractFieldName, hitField);
                        }
                        for (Object value : values) {
                            hitField.values().add(value);
                        }
                    }
                }
            }

//            hitContext.reset(searchHit, subReaderContext, subDoc, context.searchContext().searcher().getIndexReader(), docId, fieldsVisitor);
//            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
//                if (fetchSubPhase.hitExecutionNeeded(context)) {
//                    fetchSubPhase.hitExecute(context, hitContext);
//                }
//            }
        }
        
        if(hitList.isEmpty())
        {
            return new InternalGrowth(name, InternalSearchHits.EMPTY);
        }
        else
        {
            return new InternalGrowth(name, hitList.toArray(SearchHit.class));
        }
    }
    
    private void loadStoredFields(SearchContext context, FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            context.searcher().doc(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation()
    {
        return new InternalGrowth(name, InternalSearchHits.EMPTY);
    }

    public static class Factory extends AggregatorFactory
    {

        protected ValuesSourceConfig<ValuesSource.Numeric> orderConfig;
        protected ValuesSourceConfig<ValuesSource.Numeric> config;

        private double                       min;
        private double                       max;
        private boolean                      minIsPercentage;
        private boolean                      maxIsPercentage;

        public Factory(String name, String type, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig,
                ValuesSourceConfig<ValuesSource.Numeric> orderSourceConfig, double min,
                boolean minIsPercentage, double max, boolean maxIsPercentage)
        {
            super(name, type);
            this.config = valuesSourceConfig;
            this.orderConfig = orderSourceConfig;
            this.min = min;
            this.minIsPercentage = minIsPercentage;
            this.max = max;
            this.maxIsPercentage = maxIsPercentage;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount)
        {
            ValuesSource.Numeric valueValuesSource = context.valuesSource(config,
                    parent == null ? 0 : 1 + parent.depth());
            ValuesSource.Numeric orderValuesSource = context.valuesSource(orderConfig,
                    parent == null ? 0 : 1 + parent.depth());
            return new GrowthAggregator(name, expectedBucketsCount, valueValuesSource, orderValuesSource, context,
                    parent, min, minIsPercentage, max, maxIsPercentage);
        }
    }

    @Override
    public void doClose()
    {
        Releasables.close(comparables);
        Releasables.close(values);
        Releasables.close(ids);
    }

}
