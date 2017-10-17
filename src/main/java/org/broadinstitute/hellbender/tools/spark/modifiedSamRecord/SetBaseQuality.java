package org.broadinstitute.hellbender.tools.spark.modifiedSamRecord;


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

import java.util.Collections;
import java.util.List;


@CommandLineProgramProperties(
        summary ="setBaseQualities on Spark",
        oneLineSummary ="setBaseQualities on Spark",
        programGroup = SparkProgramGroup.class)
public final class SetBaseQuality extends GATKSparkTool {
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
    
    public static GATKRead setNewBaseQualities(GATKRead read,byte quality){
        byte[] originalQual = read.getBaseQualities();
        for(int i = 0;i<originalQual.length;i++){
            originalQual[i] = quality;
        }
        read.setBaseQualities(originalQual);
        return read;
        
    }
    @Override
    protected void runTool(final JavaSparkContext ctx) {
        JavaRDD<GATKRead> reads = getReads();
        JavaRDD<GATKRead> firstReads = reads.filter(read->read.isFirstOfPair())
                                            .map(firstRead->setNewBaseQualities(firstRead,(byte) 16));
        JavaRDD<GATKRead> secondReads = reads.filter(read->read.isSecondOfPair())
                                             .map(secondRead->setNewBaseQualities(secondRead,(byte)18));
        JavaRDD<GATKRead> wholeReads = firstReads.union(secondReads);
        
        writeReads(ctx,output,wholeReads);
    }
}
