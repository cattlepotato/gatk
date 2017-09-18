package org.broadinstitute.hellbender.tools.copynumber;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.IntervalList;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.VariantProgramGroup;
import org.broadinstitute.hellbender.engine.GATKTool;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.io.File;
import java.util.List;



@CommandLineProgramProperties(
        summary = "Split intervals into sub-interval files.",
        oneLineSummary = "Split intervals into sub-interval files.",
        programGroup = VariantProgramGroup.class
)
@DocumentedFeature
public class CreateBinningIntervals extends GATKTool {
    public static final String WIDTH_OF_BINS_SHORT_NAME = "bw";
    public static final String WIDTH_OF_BINS_LONG_NAME = "binwidths";

    public static final String PADDING_SHORT_NAME = "pad";
    public static final String PADDING_LONG_NAME = "padding";

    @Argument(
            doc = "width of the bins",
            fullName = WIDTH_OF_BINS_LONG_NAME,
            shortName = WIDTH_OF_BINS_SHORT_NAME,
            optional = true,
            minValue = 1
    )
    private int widthOfBins = 1;

    @Argument(
            doc = "width of the padding regions",
            fullName = PADDING_LONG_NAME,
            shortName = PADDING_SHORT_NAME,
            optional = true,
            minValue = 0
    )
    private int padding = 0;

    @Argument(
            doc = "output file",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            optional = false
    )
    public File outputFile;



    @Override
    public void onTraversalStart() {
        createBins();
    }

    /**
     * Generates binning coverage in the intervals given by the user.
     * The width of bins, the intervals and the output file's path are given by the user.
     */
    public void createBins() {
        // check if the output directory exists
        if (!outputFile.exists() && !outputFile.mkdir()) {
            throw new RuntimeException("Unable to create file: " + outputFile.getAbsolutePath());
        }

        // check if the bin widths are set appropriately
        if(widthOfBins <= 0) {
            throw new IllegalArgumentException("Width of bins " + Integer.toString(widthOfBins) + " should be >= 0.");
        }

        // get the sequence dictionary
        final SAMSequenceDictionary sequenceDictionary = getBestAvailableSequenceDictionary();
        final List<SimpleInterval> intervals = hasIntervals() ? intervalArgumentCollection.getIntervals(sequenceDictionary)
               : IntervalUtils.getAllIntervalsForReference(sequenceDictionary);

        // create an IntervalList by copying all elements of 'intervals' into it
        IntervalList intervalList = new IntervalList(sequenceDictionary);
        intervals.stream().map(si -> new Interval(si.getContig(), si.getStart(), si.getEnd())).forEach(intervalList::add);

        // sort intervals according to their coordinates and unique them (i.e. delete duplicates)
        intervalList.uniqued();

        // pad all elements of intervalList
        intervalList = intervalList.padded(padding,padding);

        // merge those that intersect after padding
        intervalList = IntervalList.intersection(intervalList, intervalList);

        // break the intervals up to bins -- the last bin in each interval can be shorter than the others
        IntervalList bins = new IntervalList(sequenceDictionary);
        int bin_start, bin_end;
        Interval new_bin;
        for(Interval in : intervalList) {
            bin_start = in.getStart();
            bin_end = Math.min(bin_start + widthOfBins - 1, in.getEnd());
            while(bin_start < in.getEnd()) {
                new_bin = new Interval(in.getContig(), bin_start, bin_end);
                bins.add(new_bin);
                bin_start += widthOfBins;
                bin_end = Math.min(bin_start + widthOfBins - 1, in.getEnd());
            }
        }

        // write the bins into file
        bins.write(outputFile);
    }

    @Override
    public void traverse() { }  // no traversal for this tool!
}


