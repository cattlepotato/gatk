package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.EvidenceTargetLink;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.PairedStrandedIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.StrandedInterval;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.test.VariantContextTestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.when;

public class DiscoverVariantsFromContigAlignmentsSAMSparkUnitTest extends BaseTest {


    private final Logger localLogger = LogManager.getLogger(DiscoverVariantsFromContigAlignmentsSAMSparkUnitTest.class);
    private static String twoBitRefURL = publicTestDir + "large/human_g1k_v37.20.21.2bit";

    @DataProvider(name = "evidenceTargetLinksAndVariants")
    public Object[][] getEvidenceTargetLinksAndVariants() {
        final VariantContext unAnnotatedVC = new VariantContextBuilder()
                .id("TESTID")
                .chr("20").start(200).stop(300)
                .alleles("A", "<" + GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL_IN_HEADER + ">")
                .attribute(VCFConstants.END_KEY, 300)
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.toString())
                .make();

        final VariantContext annotatedVC = new VariantContextBuilder()
                .id("TESTID")
                .chr("20").start(200).stop(300)
                .alleles("A", "<" + GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL_IN_HEADER + ">")
                .attribute(VCFConstants.END_KEY, 300)
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.toString())
                .attribute(GATKSVVCFConstants.READ_PAIR_SUPPORT, 7)
                .attribute(GATKSVVCFConstants.SPLIT_READ_SUPPORT, 5)
                .make();

        final VariantContext impreciseDeletion = new VariantContextBuilder()
                .id("DEL_IMPRECISE_20_950_1050_1975_2025")
                .chr("20").start(1000).stop(2000)
                .alleles("N", "<" + GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL_IN_HEADER + ">")
                .attribute(VCFConstants.END_KEY, 2000)
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.toString())
                .attribute(GATKSVVCFConstants.READ_PAIR_SUPPORT, 7)
                .attribute(GATKSVVCFConstants.SPLIT_READ_SUPPORT, 5)
                .attribute(GATKSVVCFConstants.IMPRECISE, true)
                .attribute(GATKSVVCFConstants.CIPOS, "-50,50")
                .attribute(GATKSVVCFConstants.CIEND, "-25,25")
                .attribute(GATKSVVCFConstants.SVLEN, -1000)
                .make();

        List<Object[]> tests = new ArrayList<>();
        tests.add(new Object[] {
                Arrays.asList(
                        new EvidenceTargetLink(
                                new StrandedInterval(new SVInterval(0, 190, 210), true),
                                new StrandedInterval(new SVInterval(0, 310, 320), false),
                                5, 7, new HashSet<>(), new HashSet<>())),
                Arrays.asList( unAnnotatedVC ),
                Arrays.asList( annotatedVC ) }
                );
        tests.add(new Object[] {
                Arrays.asList(
                        new EvidenceTargetLink(
                                new StrandedInterval(new SVInterval(0, 190, 210), true),
                                new StrandedInterval(new SVInterval(0, 310, 320), true),
                                5, 7, new HashSet<>(), new HashSet<>())),
                Arrays.asList( unAnnotatedVC ),
                Arrays.asList( unAnnotatedVC ) }
        );
        tests.add(new Object[] {
                Arrays.asList(
                        new EvidenceTargetLink(
                                new StrandedInterval(new SVInterval(0, 950, 1050), true),
                                new StrandedInterval(new SVInterval(0, 1975, 2025), false),
                                5, 7, new HashSet<>(), new HashSet<>())),
                Arrays.asList( unAnnotatedVC ),
                Arrays.asList( unAnnotatedVC, impreciseDeletion ) }
        );
        tests.add(new Object[] {
                Arrays.asList(
                        new EvidenceTargetLink(
                                new StrandedInterval(new SVInterval(0, 190, 210), true),
                                new StrandedInterval(new SVInterval(0, 310, 320), false),
                                3, 4, new HashSet<>(), new HashSet<>()),
                        new EvidenceTargetLink(
                                new StrandedInterval(new SVInterval(0, 192, 215), true),
                                new StrandedInterval(new SVInterval(0, 299, 303), false),
                                2, 3, new HashSet<>(), new HashSet<>())),
                Arrays.asList( unAnnotatedVC ),
                Arrays.asList( annotatedVC ) }
        );

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "evidenceTargetLinksAndVariants")
    public void testProcessEvidenceTargetLinks(final List<EvidenceTargetLink> etls,
                                               final List<VariantContext> inputVariants,
                                               final List<VariantContext> expectedVariants) throws Exception {
        final StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection params =
                new StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection();

        final PipelineOptions options = null;
        final ReferenceMultiSource referenceMultiSource = new ReferenceMultiSource(options, twoBitRefURL, ReferenceWindowFunctions.IDENTITY_FUNCTION);

        ReadMetadata metadata = Mockito.mock(ReadMetadata.class);
        when(metadata.getMaxMedianFragmentSize()).thenReturn(300);


        PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTree = new PairedStrandedIntervalTree<>();
        etls.forEach(e -> evidenceTree.put(e.getPairedStrandedIntervals(), e));

        final List<VariantContext> processedVariantContexts =
                DiscoverVariantsFromContigAlignmentsSAMSpark.processEvidenceTargetLinks(params, localLogger, evidenceTree,
                        metadata, inputVariants, referenceMultiSource);

        Assert.assertEquals(processedVariantContexts.size(), expectedVariants.size());
        for (int i = 0; i < processedVariantContexts.size(); i++) {
            VariantContextTestUtils.assertVariantContextsAreEqual(processedVariantContexts.get(i), expectedVariants.get(i), new ArrayList<>());
        }
    }

}