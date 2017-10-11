package org.broadinstitute.hellbender.tools.spark.transforms.markduplicates;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.MetricsFile;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.OpticalDuplicatesArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.markduplicates.DuplicationMetrics;
import org.broadinstitute.hellbender.utils.read.markduplicates.MarkDuplicatesScoringStrategy;
import org.broadinstitute.hellbender.utils.read.markduplicates.OpticalDuplicateFinder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@CommandLineProgramProperties(
        summary ="Marks duplicates on spark",
        oneLineSummary ="MarkDuplicates on Spark",
        programGroup = SparkProgramGroup.class)
@BetaFeature
public final class MarkDuplicatesSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return true; }

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    protected String output;

    @Argument(doc = "Path to write duplication metrics to.", optional=true,
            shortName = "M", fullName = "METRICS_FILE")
    protected String metricsFile;

    @Argument(shortName = "DS", fullName = "DUPLICATE_SCORING_STRATEGY", doc = "The scoring strategy for choosing the non-duplicate among candidates.")
    public MarkDuplicatesScoringStrategy duplicatesScoringStrategy = MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES;

    @Argument(shortName = "NQ", fullName = "NAME_QUAL", doc = "A file contain name and qulities.",optional = false)
    public String nameANDqual;

    @ArgumentCollection
    protected OpticalDuplicatesArgumentCollection opticalDuplicatesArgumentCollection = new OpticalDuplicatesArgumentCollection();

    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        return Collections.singletonList(ReadFilterLibrary.ALLOW_ALL_READS);
    }

    public static JavaRDD<GATKRead> mark(JavaPairRDD<String,List<byte[]>> nameAndQual, final JavaRDD<GATKRead> reads, final SAMFileHeader header,
                                         final MarkDuplicatesScoringStrategy scoringStrategy,
                                         final OpticalDuplicateFinder opticalDuplicateFinder, final int numReducers) {

        JavaRDD<GATKRead> primaryReads = reads.filter(v1 -> !ReadUtils.isNonPrimary(v1));
        JavaRDD<GATKRead> nonPrimaryReads = reads.filter(v1 -> ReadUtils.isNonPrimary(v1));
        JavaRDD<GATKRead> primaryReadsTransformed = MarkDuplicatesSparkUtils.transformReads(nameAndQual, header, scoringStrategy, opticalDuplicateFinder, primaryReads, numReducers);

        return primaryReadsTransformed.union(nonPrimaryReads);
    }
    public static JavaRDD<GATKRead> mark(final JavaRDD<GATKRead> reads, final SAMFileHeader header,
                                         final MarkDuplicatesScoringStrategy scoringStrategy,
                                         final OpticalDuplicateFinder opticalDuplicateFinder, final int numReducers) {

        JavaRDD<GATKRead> primaryReads = reads.filter(v1 -> !ReadUtils.isNonPrimary(v1));
        JavaRDD<GATKRead> nonPrimaryReads = reads.filter(v1 -> ReadUtils.isNonPrimary(v1));
        JavaRDD<GATKRead> primaryReadsTransformed = MarkDuplicatesSparkUtils.transformReads(header, scoringStrategy, opticalDuplicateFinder, primaryReads, numReducers);

        return primaryReadsTransformed.union(nonPrimaryReads);
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        JavaRDD<GATKRead> reads = getReads();
        final OpticalDuplicateFinder finder = opticalDuplicatesArgumentCollection.READ_NAME_REGEX != null ?
                new OpticalDuplicateFinder(opticalDuplicatesArgumentCollection.READ_NAME_REGEX, opticalDuplicatesArgumentCollection.OPTICAL_DUPLICATE_PIXEL_DISTANCE, null) : null;
        
//        JavaRDD<String> qual = ctx.textFile("/home/jryoung/documents/gatk_beta4_5/gatk_beta4_5/NA12878/testqual");
//        JavaRDD<String> qual = ctx.textFile("hdfs:///user/liucheng/NA12878/NA12878_500w_merge_nameQual.fastq");
        JavaRDD<String> qual = ctx.textFile(nameANDqual);
        JavaPairRDD<String,List<byte[]>>  nameAndQual = qual.mapToPair(line->{
//            String [] a = line.split(",");
            String [] a = line.split("\\|");
            List<byte[]> baseQual = new ArrayList<>(2);
            for(int k = 1;k<=2;k++){
                byte[] realqual  = a[k].getBytes();
                for(int j=0;j<realqual.length;j++){
                    realqual[j]=(byte)((int)realqual[j]-33);
                }
                baseQual.add(realqual);
            }

            return new Tuple2<>(a[0], baseQual);

        });
        final JavaRDD<GATKRead> finalReadsForMetrics = mark(nameAndQual,reads, getHeaderForReads(), duplicatesScoringStrategy, finder, getRecommendedNumReducers());
//        final JavaRDD<GATKRead> finalReadsForMetrics = mark(reads, getHeaderForReads(), duplicatesScoringStrategy, finder, getRecommendedNumReducers());

        if (metricsFile != null) {
            final JavaPairRDD<String, DuplicationMetrics> metricsByLibrary = MarkDuplicatesSparkUtils.generateMetrics(getHeaderForReads(), finalReadsForMetrics);
            final MetricsFile<DuplicationMetrics, Double> resultMetrics = getMetricsFile();
            MarkDuplicatesSparkUtils.saveMetricsRDD(resultMetrics, getHeaderForReads(), metricsByLibrary, metricsFile, getAuthHolder());
        }

        final JavaRDD<GATKRead> finalReads = cleanupTemporaryAttributes(finalReadsForMetrics);
        writeReads(ctx, output, finalReads);
    }


    /**
     * The OD attribute was added to each read for optical dups.
     * Now we have to clear it to avoid polluting the output.
     */
    public static JavaRDD<GATKRead> cleanupTemporaryAttributes(final JavaRDD<GATKRead> reads) {
        return reads.map(read -> {
            read.clearAttribute(MarkDuplicatesSparkUtils.OPTICAL_DUPLICATE_TOTAL_ATTRIBUTE_NAME);
            return read;
        });
    }
}
