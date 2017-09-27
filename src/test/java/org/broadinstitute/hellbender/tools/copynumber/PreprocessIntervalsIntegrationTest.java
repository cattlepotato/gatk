package org.broadinstitute.hellbender.tools.copynumber;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import htsjdk.samtools.util.IntervalList;
import htsjdk.samtools.util.Interval;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;

public final class PreprocessIntervalsIntegrationTest extends CommandLineProgramTest {
    private static final File REFERENCE = new File(b37_reference_20_21);

    @DataProvider(name = "intervalInputsFromCommandLine")
    public final Object[][] testData() {
        // Test for separate intervals
        final int binLengthSeparateIntervalTest = 10_000;
        final int paddingSeparateIntervalTest = 0;
        final String[] outputFileNameSeparateIntervalTest =  {"GATK-preprocess-intervals-test", ".tmp"};
        final ArrayList<Interval> inputIntervalsSeparateIntervalTest = new ArrayList<Interval>();
        inputIntervalsSeparateIntervalTest.add(new Interval("20", 3_000, 20_000));
        inputIntervalsSeparateIntervalTest.add(new Interval("20", 200, 1_900));
        final ArrayList<Interval> expectedBinsSeparateIntervalTest = new ArrayList<Interval>();
        expectedBinsSeparateIntervalTest.add(new Interval("20", 200, 1_900));
        expectedBinsSeparateIntervalTest.add(new Interval("20", 3_000, 12_999));
        expectedBinsSeparateIntervalTest.add(new Interval("20", 13_000, 20_000));

        // Test for overlapping intervals
        final int binLengthOverlappingIntervalTest = 10_000;
        final int paddingOverlappingIntervalTest = 500;
        final String[] outputFileNameOverlappingIntervalTest =  {"GATK-preprocess-intervals-test", ".tmp"};
        final ArrayList<Interval> inputIntervalsOverlappingIntervalTest = new ArrayList<Interval>();
        inputIntervalsOverlappingIntervalTest.add(new Interval("20", 3_000, 20_000));
        inputIntervalsOverlappingIntervalTest.add(new Interval("20", 200, 2_100));
        final ArrayList<Interval> expectedBinsOverlappingIntervalTest = new ArrayList<Interval>();
        expectedBinsOverlappingIntervalTest.add(new Interval("20", 1, 10_000));
        expectedBinsOverlappingIntervalTest.add(new Interval("20", 10_001, 20_000));
        expectedBinsOverlappingIntervalTest.add(new Interval("20", 20_001, 20_500));

        // Test for intervals reaching the ends of the contigs
        final int binLengthEdgeIntervalTest = 10_000;
        final int paddingEdgeIntervalTest = 500;
        final String[] outputFileNameEdgeIntervalTest =  {"GATK-preprocess-intervals-test", ".tmp"};
        final ArrayList<Interval> inputIntervalsEdgeIntervalTest = new ArrayList<Interval>();
        inputIntervalsEdgeIntervalTest.add(new Interval("20", 3000, 20000));
        inputIntervalsEdgeIntervalTest.add(new Interval("20", 63025220, 63025520));
        final ArrayList<Interval> expectedBinsEdgeIntervalTest = new ArrayList<Interval>();
        expectedBinsEdgeIntervalTest.add(new Interval("20", 2500, 12499));
        expectedBinsEdgeIntervalTest.add(new Interval("20", 12500, 20500));
        expectedBinsEdgeIntervalTest.add(new Interval("20", 63024720, 63025520));

        // Test for whole chromosome
        final int binLengthWholeChromosomeTest = 10_000_000;
        final int paddingWholeChromosomeTest = 500;
        final String[] outputFileNameWholeChromosomeTest =  {"GATK-preprocess-intervals-test", ".tmp"};
        final ArrayList<Interval> inputIntervalsWholeChromosomeTest = new ArrayList<Interval>();
        inputIntervalsWholeChromosomeTest.add(new Interval("20", 1, 63_025_520));
        final ArrayList<Interval> expectedBinsWholeChromosomeTest = new ArrayList<Interval>();
        expectedBinsWholeChromosomeTest.add(new Interval("20", 1, 10_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 10_000_001, 20_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 20_000_001, 30_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 30_000_001, 40_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 40_000_001, 50_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 50_000_001, 60_000_000));
        expectedBinsWholeChromosomeTest.add(new Interval("20", 60_000_001, 63_025_520));

        // Test for whole genome -- when we don't give any intervals, then the tool assumes that the user wants to sequence the whole genome
        final int binLengthWholeGenomeTest = 10_000_000;
        final int paddingWholeGenomeTest = 500;
        final String[] outputFileNameWholeGenomeTest =  {"GATK-preprocess-intervals-test", ".tmp"};
        final ArrayList<Interval> inputIntervalsWholeGenomeTest = new ArrayList<Interval>();
        final ArrayList<Interval> expectedBinsWholeGenomeTest = new ArrayList<Interval>();
        expectedBinsWholeGenomeTest.add(new Interval("20", 1, 10_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 10_000_001, 20_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 20_000_001, 30_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 30_000_001, 40_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 40_000_001, 50_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 50_000_001, 60_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("20", 60_000_001, 63_025_520));
        expectedBinsWholeGenomeTest.add(new Interval("21", 1, 10_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("21", 10_000_001, 20_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("21", 20_000_001, 30_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("21", 30_000_001, 40_000_000));
        expectedBinsWholeGenomeTest.add(new Interval("21", 40_000_001, 48_129_895));

        // Return all test data
        return new Object[][]{
                {binLengthSeparateIntervalTest, paddingSeparateIntervalTest, outputFileNameSeparateIntervalTest, inputIntervalsSeparateIntervalTest, expectedBinsSeparateIntervalTest},
                {binLengthOverlappingIntervalTest, paddingOverlappingIntervalTest, outputFileNameOverlappingIntervalTest, inputIntervalsOverlappingIntervalTest, expectedBinsOverlappingIntervalTest},
                {binLengthEdgeIntervalTest, paddingEdgeIntervalTest, outputFileNameEdgeIntervalTest, inputIntervalsEdgeIntervalTest, expectedBinsEdgeIntervalTest},
                {binLengthWholeChromosomeTest, paddingWholeChromosomeTest, outputFileNameWholeChromosomeTest, inputIntervalsWholeChromosomeTest, expectedBinsWholeChromosomeTest},
                {binLengthWholeGenomeTest, paddingWholeGenomeTest, outputFileNameWholeGenomeTest, inputIntervalsWholeGenomeTest, expectedBinsWholeGenomeTest}
        };
    }

    // Test for interval inputs given as -L command line arguments
    @Test(dataProvider = "intervalInputsFromCommandLine")
    public final void testCommandLine(final int binLength, final int padding,  final String[] outputFileName, final ArrayList<Interval> inputIntervals, final ArrayList<Interval> expectedBins) {
        final File outputFile = createTempFile(outputFileName[0], outputFileName[1]);
        final ArrayList<String> args = new ArrayList<>(Arrays.asList(
                "-R", REFERENCE.getAbsolutePath(),
                "-" + PreprocessIntervals.LENGTH_OF_BINS_SHORT_NAME, Integer.toString(binLength),
                "-" + PreprocessIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        ));
        for (Interval interval : inputIntervals) {
            args.add("-L");
            args.add(interval.getContig() + ":" + Integer.toString(interval.getStart()) + "-" + Integer.toString(interval.getEnd()));
        }

        runCommandLine(args.toArray(new String[0]));

        final IntervalList binsResult = IntervalList.fromFile(outputFile);
        Assert.assertEquals(binsResult, expectedBins);
    }

    // Test for interval inputs read from a file
    @Test
    public final void singleFileTest() {
        final int binLength = 10000;
        final int padding = 5000;
        final File outputFile = createTempFile("GATK-preprocess-intervals-test", ".tmp");
        final String[] args = {
                "-L", BaseTest.packageRootTestDir + "tools/copynumber/preprocess-intervals-test.interval_list",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + PreprocessIntervals.LENGTH_OF_BINS_SHORT_NAME, Integer.toString(binLength),
                "-" + PreprocessIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        IntervalList binsResult = IntervalList.fromFile(outputFile);

        IntervalList binsExpected = new IntervalList(binsResult.getHeader().getSequenceDictionary());
        binsExpected.add(new Interval("20", 1, 10000));
        binsExpected.add(new Interval("20", 10001, 16000));
        binsExpected.add(new Interval("21", 1, 5100));
        binsExpected.add(new Interval("21", 15000, 24999));
        binsExpected.add(new Interval("21", 25000, 27000));

        Assert.assertEquals(binsResult, binsExpected);
    }
}