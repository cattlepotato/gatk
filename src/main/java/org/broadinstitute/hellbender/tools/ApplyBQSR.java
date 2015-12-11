package org.broadinstitute.hellbender.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.transformers.BQSRReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;

import java.io.File;

@CommandLineProgramProperties(
        summary = "Applies the BQSR table to the input SAM/BAM/CRAM",
        oneLineSummary = "Applies the BQSR table to the input SAM/BAM/CRAM",
        programGroup = ReadProgramGroup.class
)
public final class ApplyBQSR extends ReadWalker{

    private static final Logger logger = LogManager.getLogger(ApplyBQSR.class);

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    /**
     * Enables recalibration of base qualities.
     * The covariates tables are produced by the BaseRecalibrator tool.
     * Please be aware that you should only run recalibration with the covariates file created on the same input bam(s).
     */
    @Argument(fullName="bqsr_recal_file", shortName="bqsr", doc="Input covariates table file for base quality score recalibration")
    public File BQSR_RECAL_FILE;

    /**
     * command-line arguments to fine tune the recalibration.
     */
    @ArgumentCollection
    public ApplyBQSRArgumentCollection bqsrArgs = new ApplyBQSRArgumentCollection();
    
    private SAMFileGATKReadWriter outputWriter;

    private ReadTransformer transform;

    @Override
    public void onTraversalStart() {
        outputWriter = createSAMWriter(OUTPUT, true);
        transform = new BQSRReadTransformer(getHeaderForReads(), BQSR_RECAL_FILE, bqsrArgs);
        Utils.warnOnNonIlluminaReadGroups(getHeaderForReads(), logger);
    }

    @Override
    public void apply( GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        outputWriter.addRead(transform.apply(read));
    }

    @Override
    public Object onTraversalDone() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
        return null;
    }

}