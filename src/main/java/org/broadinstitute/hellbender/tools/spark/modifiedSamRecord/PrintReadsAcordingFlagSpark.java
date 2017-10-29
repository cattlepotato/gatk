package org.broadinstitute.hellbender.tools.spark.modifiedSamRecord;


import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.markduplicates.ReadsKey;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;


@CommandLineProgramProperties(
        summary ="setBaseQualities on Spark",
        oneLineSummary ="setBaseQualities on Spark",
        programGroup = SparkProgramGroup.class)
public final class PrintReadsAcordingFlagSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        return Collections.singletonList(ReadFilterLibrary.ALLOW_ALL_READS);
    }

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    protected String output;
    

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        JavaRDD<GATKRead> reads = getReads();
        JavaRDD<GATKRead> primaryReads = reads.filter(v1 -> !ReadUtils.isNonPrimary(v1));
        JavaPairRDD<String, GATKRead> keyReadPairs = primaryReads.mapToPair(read -> new Tuple2<>(ReadsKey.keyForRead(read), read));
        JavaPairRDD<String, Iterable<GATKRead>> keyedReads = keyReadPairs.groupByKey();
        JavaPairRDD<String, Iterable<GATKRead>> filtedMultiReads =  keyedReads.filter(keyedRead->{
            int i =0;
            for (GATKRead read : keyedRead._2()){
                i++;
            }
            if(i>2)
                return true;
            return false;
        });
        System.out.println("mult read\n"+filtedMultiReads.count());
//        JavaRDD<String> key = filtedMultiReads.map(read->read._1());
//
//        key.saveAsTextFile(output);
        JavaRDD<GATKRead> finalRead = filtedMultiReads.flatMap(keyRead->{
            List<GATKRead> out = Lists.newArrayList();
            for (GATKRead read : keyRead._2()){
                out.add(read);
            }
            return out.iterator();
        });





        writeReads(ctx,output,finalRead);
    }
}
