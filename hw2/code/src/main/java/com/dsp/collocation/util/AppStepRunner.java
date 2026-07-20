package com.dsp.collocation.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AppStepRunner {

    private static final Regions REGION = Regions.US_EAST_1;

    // change if you want
    private static final String BUCKET = util_class.getbucketName();

    // datasets
    private static final String HEB_BIGRAM_INPUT =
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static final String HEB_UNIGRAM_INPUT =
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";

    private static final String ENG_BIGRAM_INPUT =
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data";
    private static final String ENG_UNIGRAM_INPUT =
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data";

    private static final String LOCAL_FAT_JAR_PATH =
            "target/CollocationExtraction_Project-1.0-SNAPSHOT-jar-with-dependencies.jar";

    private static final String S3_JAR_KEY =
            "jars/CollocationExtraction_Project-1.0-SNAPSHOT-jar-with-dependencies.jar";

    private static String s3Uri(String bucket, String keyOrPrefix) {
        return "s3://" + bucket + "/" + keyOrPrefix;
    }

    public static void main(String[] args) {

        // arg0: WITH/WITHOUT
        String rawMode = (args.length > 0) ? args[0] : "WITH";
        CombinerUsage mode = CombinerUsage.fromString(rawMode);
        String suffix = (mode == CombinerUsage.WITH) ? "with" : "without";

        // arg1: ENG/HEB (optional)
        String lang = (args.length > 1) ? args[1].trim().toUpperCase() : "ENG";
        boolean useEnglish = !lang.equals("HEB");

        String bigramInput = useEnglish ? ENG_BIGRAM_INPUT : HEB_BIGRAM_INPUT;
        String unigramInput = useEnglish ? ENG_UNIGRAM_INPUT : HEB_UNIGRAM_INPUT;

        // Unique output prefix per run => fixes "File already exists"
        String runId = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date())
                + "-" + UUID.randomUUID().toString().substring(0, 8);

        String runPrefix = "runs/" + (useEnglish ? "eng" : "heb") + "/" + runId + "/";

        AWSCredentialsProvider creds = new ProfileCredentialsProvider();

        // --- S3 ---
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .withCredentials(creds)
                .build();

        ensureBucketExistsUsEast1Only(s3, BUCKET);
        uploadJarToS3(s3, BUCKET, S3_JAR_KEY, LOCAL_FAT_JAR_PATH);

        String fatJarS3 = s3Uri(BUCKET, S3_JAR_KEY);

        // outputs
        String STEP1_OUT = s3Uri(BUCKET, runPrefix + "step1-output-" + suffix + "/");
        String STEP2_OUT = s3Uri(BUCKET, runPrefix + "step2-output-" + suffix + "/");
        String STEP3_OUT = s3Uri(BUCKET, runPrefix + "step3-output-" + suffix + "/");
        String STEP4_OUT = s3Uri(BUCKET, runPrefix + "step4-output-" + suffix + "/");

        // --- EMR ---
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(REGION)
                .withCredentials(creds)
                .build();

        StepConfig step1 = createStep(
                "Step1 (" + suffix + ")",
                fatJarS3,
                bigramInput,
                null,
                STEP1_OUT,
                "com.dsp.collocation.Step1",
                mode.name()
        );

        StepConfig step2 = createStep(
                "Step2 (" + suffix + ")",
                fatJarS3,
                unigramInput,
                null,
                STEP2_OUT,
                "com.dsp.collocation.Step2",
                mode.name()
        );

        StepConfig step3 = createStep(
                "Step3 (Join+LLR)",
                fatJarS3,
                STEP1_OUT,
                STEP2_OUT,
                STEP3_OUT,
                "com.dsp.collocation.step3",
                null
        );

        StepConfig step4 = createStep(
                "Step4 (" + suffix + ")",
                fatJarS3,
                STEP3_OUT,
                null,
                STEP4_OUT,
                "com.dsp.collocation.step4",
                mode.name()
        );

        // --- Cluster-wide MR tuning (safe defaults) ---
        Map<String, String> props = new HashMap<>();

        // maps
        props.put("mapreduce.map.memory.mb", "4096");
        props.put("mapreduce.map.java.opts", "-Xmx3072m");

        // reduces (Step3 is reducer-heavy)
        props.put("mapreduce.reduce.memory.mb", "8192");
        props.put("mapreduce.reduce.java.opts", "-Xmx6500m");

        props.put("yarn.app.mapreduce.am.resource.mb", "2048");
        props.put("yarn.app.mapreduce.am.command-opts", "-Xmx1536m");

        // helps on S3 committers
        props.put("mapreduce.fileoutputcommitter.algorithm.version", "2");
        props.put("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true");

        Configuration mapredSite = new Configuration()
                .withClassification("mapred-site")
                .withProperties(props);

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(7)                 // 1 master + 6 core
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest req = new RunJobFlowRequest()
                .withName("Collocation-Extraction " + (useEnglish ? "ENG" : "HEB") + " (" + suffix + ")")
                .withInstances(instances)
                .withReleaseLabel("emr-6.10.0")
                .withConfigurations(mapredSite)
                .withSteps(step1, step2, step3, step4)
                .withLogUri(s3Uri(BUCKET, runPrefix + "logs/"))
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        try {
            RunJobFlowResult res = emr.runJobFlow(req);
            System.out.println("[INFO] Cluster started. JobFlowId = " + res.getJobFlowId());
            System.out.println("[INFO] Language: " + (useEnglish ? "ENG" : "HEB"));
            System.out.println("[INFO] RunId: " + runId);
            System.out.println("[INFO] Outputs:");
            System.out.println("  Step1: " + STEP1_OUT);
            System.out.println("  Step2: " + STEP2_OUT);
            System.out.println("  Step3: " + STEP3_OUT);
            System.out.println("  Step4: " + STEP4_OUT);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to run job flow: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static StepConfig createStep(
            String name,
            String jarS3Uri,
            String input,
            String input2,
            String output,
            String mainClass,
            String combinerModeOrNull) {

        HadoopJarStepConfig hadoop;

        if (input2 == null) {
            if (combinerModeOrNull != null) {
                hadoop = new HadoopJarStepConfig()
                        .withJar(jarS3Uri)
                        .withMainClass(mainClass)
                        .withArgs(input, output, combinerModeOrNull);
            } else {
                hadoop = new HadoopJarStepConfig()
                        .withJar(jarS3Uri)
                        .withMainClass(mainClass)
                        .withArgs(input, output);
            }
        } else {
            hadoop = new HadoopJarStepConfig()
                    .withJar(jarS3Uri)
                    .withMainClass(mainClass)
                    .withArgs(input, input2, output);
        }

        return new StepConfig()
                .withName(name)
                .withHadoopJarStep(hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }

    private static void ensureBucketExistsUsEast1Only(AmazonS3 s3, String bucket) {
        if (s3.doesBucketExistV2(bucket)) {
            System.out.println("[INFO] Bucket exists: " + bucket);
            return;
        }
        System.out.println("[INFO] Creating bucket (us-east-1): " + bucket);
        s3.createBucket(bucket);
        System.out.println("[INFO] Bucket created: " + bucket);
    }

    private static void uploadJarToS3(AmazonS3 s3, String bucket, String key, String localJarPath) {
        File jar = new File(localJarPath);
        if (!jar.exists() || !jar.isFile()) {
            throw new RuntimeException(
                    "Local jar not found: " + localJarPath +
                    "\nBuild it first: mvn clean package"
            );
        }
        System.out.println("[INFO] Uploading jar to s3://" + bucket + "/" + key);
        s3.putObject(new PutObjectRequest(bucket, key, jar));
        System.out.println("[INFO] Upload done.");
    }
}
