package com.tr.ap.es.agg.order;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.hppc.DoubleArrayList;
import org.elasticsearch.common.hppc.IntArrayList;
import org.elasticsearch.common.hppc.LongArrayList;

public class ParallelCollectionUtilities
{
    public static void sort(LongArrayList list, DoubleArrayList parallelList, IntArrayList parallelList2) {
        sort(list.buffer, parallelList.buffer, parallelList2.buffer, list.size());
    }

    public static void sort(final long[] array, final double[] parallelArray, final int[] parallelArray2, int len) {
        new IntroSorter() {

            long pivot;

            @Override
            protected void swap(int i, int j) {
                final long tmp = array[i];
                array[i] = array[j];
                array[j] = tmp;
                
                final double ptmp = parallelArray[i];
                parallelArray[i] = parallelArray[j];
                parallelArray[j] = ptmp;
                
                final int ptmp2 = parallelArray2[i];
                parallelArray2[i] = parallelArray2[j];
                parallelArray2[j] = ptmp2;
            }

            @Override
            protected int compare(int i, int j) {
                return Long.compare(array[i], array[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Long.compare(pivot, array[j]);
            }

        }.sort(0, len);
    }
}
