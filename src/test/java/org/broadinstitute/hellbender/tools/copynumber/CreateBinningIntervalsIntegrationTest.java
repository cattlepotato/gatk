package org.broadinstitute.hellbender.tools.copynumber;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.engine.ReferenceDataSource;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.testng.Assert;
import org.testng.annotations.Test;
import htsjdk.samtools.util.IntervalList;
import htsjdk.samtools.util.Interval;

import java.io.File;
import java.io.*;
import java.io.IOException;


public class CreateBinningIntervalsIntegrationTest extends CommandLineProgramTest {

    private static final File REFERENCE = new File(b37_reference_20_21);
    private static final GenomeLocParser GLP = new GenomeLocParser(ReferenceDataSource.of(REFERENCE).getSequenceDictionary());

    // Test for separate intervals
    @Test
    public void simpleIntervalTest1() {
        final int widthOfBins = 10000;
        final int padding = 500;
        final File outputFile = createTempFile("GATK-create-binning-intervals-test", ".tmp");
        final String[] args = {
                "-L", "20:3000-20000",
                "-L", "20:200-1900",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + CreateBinningIntervals.WIDTH_OF_BINS_SHORT_NAME, Integer.toString(widthOfBins),
                "-" + CreateBinningIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        // read the contents of the output file and print them out
        // printOutputFile(outputFile);

        // read the bins from the output file
        IntervalList binsFromFile = IntervalList.fromFile(outputFile);

        // we expect the following result
        IntervalList binsExpected = new IntervalList(binsFromFile.getHeader().getSequenceDictionary());
        String contig = binsFromFile.iterator().next().getContig();
        binsExpected.add(new Interval(contig, 1, 2400));
        binsExpected.add(new Interval(contig, 2500, 12499));
        binsExpected.add(new Interval(contig, 12500, 20500));

        // compare the file contents to the expected result
        Assert.assertEquals(binsFromFile, binsExpected);
    }


    // Test for overlapping intervals
    @Test
    public void simpleIntervalTest2() {
        final int widthOfBins = 10000;
        final int padding = 500;
        final File outputFile = createTempFile("GATK-create-binning-intervals-test", ".tmp");
        final String[] args = {
                "-L", "20:3000-20000",
                "-L", "20:200-2100",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + CreateBinningIntervals.WIDTH_OF_BINS_SHORT_NAME, Integer.toString(widthOfBins),
                "-" + CreateBinningIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        // read the contents of the output file and print them out
        // printOutputFile(outputFile);

        // read the bins from the output file
        IntervalList binsFromFile = IntervalList.fromFile(outputFile);

        // we expect the following result
        IntervalList binsExpected = new IntervalList(binsFromFile.getHeader().getSequenceDictionary());
        String contig = binsFromFile.iterator().next().getContig();
        binsExpected.add(new Interval(contig, 1, 10000));
        binsExpected.add(new Interval(contig, 10001, 20000));
        binsExpected.add(new Interval(contig, 20001, 20500));

        // compare the file contents to the expected result
        Assert.assertEquals(binsFromFile, binsExpected);
    }


    // Test for intervals reaching the ends of the reads
    @Test
    public void simpleIntervalTest3() {
        final int widthOfBins = 10000;
        final int padding = 500;
        final File outputFile = createTempFile("GATK-create-binning-intervals-test", ".tmp");
        final String[] args = {
                "-L", "20:3000-20000",
                "-L", "20:63025220-63025520",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + CreateBinningIntervals.WIDTH_OF_BINS_SHORT_NAME, Integer.toString(widthOfBins),
                "-" + CreateBinningIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        // read the contents of the output file and print them out
        // printOutputFile(outputFile);

        // read the bins from the output file
        IntervalList binsFromFile = IntervalList.fromFile(outputFile);

        // we expect the following result
        IntervalList binsExpected = new IntervalList(binsFromFile.getHeader().getSequenceDictionary());
        String contig = binsFromFile.iterator().next().getContig();
        binsExpected.add(new Interval(contig, 2500, 12499));
        binsExpected.add(new Interval(contig, 12500, 20500));
        binsExpected.add(new Interval(contig, 63024720, 63025520));

        // compare the file contents to the expected result
        Assert.assertEquals(binsFromFile, binsExpected);
    }

    // Test for whole genome
    @Test
    public void simpleIntervalTest4() {
        final int widthOfBins = 10_000_000;
        final int padding = 500;
        final File outputFile = createTempFile("GATK-create-binning-intervals-test", ".tmp");
        final String[] args = {
                "-L", "20:1-63025520",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + CreateBinningIntervals.WIDTH_OF_BINS_SHORT_NAME, Integer.toString(widthOfBins),
                "-" + CreateBinningIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        // read the contents of the output file and print them out
        // printOutputFile(outputFile);

        // read the bins from the output file
        IntervalList binsFromFile = IntervalList.fromFile(outputFile);

        // we expect the following result
        IntervalList binsExpected = new IntervalList(binsFromFile.getHeader().getSequenceDictionary());
        String contig = binsFromFile.iterator().next().getContig();
        binsExpected.add(new Interval(contig, 1, 10000000));
        binsExpected.add(new Interval(contig, 10000001, 20000000));
        binsExpected.add(new Interval(contig, 20000001, 30000000));
        binsExpected.add(new Interval(contig, 30000001, 40000000));
        binsExpected.add(new Interval(contig, 40000001, 50000000));
        binsExpected.add(new Interval(contig, 50000001, 60000000));
        binsExpected.add(new Interval(contig, 60000001, 63025520));

        // compare the file contents to the expected result
        Assert.assertEquals(binsFromFile, binsExpected);
    }


    // Test for reading intervals from a file
    @Test
    public void singleFileTest5() {
        final int widthOfBins = 10000;
        final int padding = 5000;
        final File outputFile = createTempFile("GATK-create-binning-intervals-test", ".tmp");
        final String[] args = {
                "-L", "src/test/resources/org/broadinstitute/hellbender/tools/copynumber/interval-preprocessor-intervals.interval_list",
                "-R", REFERENCE.getAbsolutePath(),
                "-" + CreateBinningIntervals.WIDTH_OF_BINS_SHORT_NAME, Integer.toString(widthOfBins),
                "-" + CreateBinningIntervals.PADDING_SHORT_NAME, Integer.toString(padding),
                "-O", outputFile.getAbsolutePath()
        };
        runCommandLine(args);

        // read the contents of the output file and print them out
        // printOutputFile(outputFile);

        // read the bins from the output file
        IntervalList binsFromFile = IntervalList.fromFile(outputFile);

        // we expect the following result
        IntervalList binsExpected = new IntervalList(binsFromFile.getHeader().getSequenceDictionary());
        binsExpected.add(new Interval("20", 1, 10000));
        binsExpected.add(new Interval("20", 10001, 16000));
        binsExpected.add(new Interval("21", 1, 5100));
        binsExpected.add(new Interval("21", 15000, 24999));
        binsExpected.add(new Interval("21", 25000, 27000));

        // compare the file contents to the expected result
        Assert.assertEquals(binsFromFile, binsExpected);

    }

    private static void printOutputFile(final File outputFile) {
        String line = null;
        String fileName = outputFile.getAbsolutePath();
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(outputFile.getAbsolutePath());

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" + fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '" + fileName + "'");
        }
    }


}