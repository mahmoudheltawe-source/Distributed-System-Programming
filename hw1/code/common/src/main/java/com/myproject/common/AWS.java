
package com.myproject.common;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class AWS {
    myLogger log = new myLogger(AWS.class);// a logger for debuge
    public static AtomicInteger backet_count = new AtomicInteger(0);
    private final S3Client s3;
    private final SqsClient sqs;

    private final Ec2Client ec2;
    public String managerJar = "manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
    public String workerJar = "worker-1.0-SNAPSHOT-jar-with-dependencies.jar";

    public static String ami = "ami-0fa3fe0fa7920f68e";// 0c02fb55956c7d316
                                                       // "ami-00e95a9222311e8ed";ami-00e95a9222311e8ed
                                                       // ami-0fa3fe0fa7920f68e

    // public static Region region1 = Region.US_WEST_2;
    public static Region region1 = Region.US_EAST_1;

    // private static final AWS instance = new AWS();

    protected static AWS instance = null;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
    }

    public static AWS getInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
    }

    public static final int MAX_TOTAL_INSTANCES = 18;// todo:check
    public String bucketName = "zaki-manager-bucket";
    public String jarFileName = "AWSCloud-1.0-SNAPSHOT-jar-with-dependencies.jar";

    // S3
    public void createBucketIfNotExists(String bucketName) {

        // check first

        // check the name
        if (bucketName == null || bucketName.isEmpty() || bucketName.equals("null")) {
            // the bucket name is inValid
            throw new IllegalArgumentException("bucketName is null or empty");
        }
        try {
            // check if exist
            s3.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            log.debug("Bucket already exists: " + bucketName);
            return; // there no need to create another
        } catch (NoSuchBucketException e) {
            // good error backet not exist the create
            log.info("Bucket does not exist, creating: " + bucketName);
        } catch (S3Exception e) {
            // there another bad error
            log.error("headBucket error: " + e.awsErrorDetails().errorMessage());
        }

        // every thing is good create
        try {
            CreateBucketRequest.Builder builder = CreateBucketRequest.builder()
                    .bucket(bucketName);

            // todo: zaki check the region
            if (!region1.equals(Region.US_EAST_1)) {
                builder = builder.createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(BucketLocationConstraint.fromValue(region1.id()))
                                .build());
            }

            s3.createBucket(builder.build());

            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());

            log.info("Bucket created: " + bucketName);
        } catch (BucketAlreadyOwnedByYouException e) {
            log.error("Bucket already owned by you: " + bucketName);
        } catch (S3Exception e) {
            log.error("[createBucketIfNotExists] " + e.awsErrorDetails().errorMessage());
            throw e; // this will show you clearly if something is still wrong
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
        if (listObjectsV2Response.contents().isEmpty()) {
            System.out.println("No objects in bucket");
            return;
        }
        List<ObjectIdentifier> keys = listObjectsV2Response.contents().stream()
                .map(con -> ObjectIdentifier.builder().key(con.key()).build()).collect(Collectors.toList());

        int bSize = 1000;
        for (int i = 0; i < keys.size(); i += bSize) {
            List<ObjectIdentifier> bKeys = keys.subList(i, Math.min(i + bSize, keys.size()));
            Delete d = Delete.builder().objects(bKeys).build();
            DeleteObjectsRequest deleteObjectRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(d)
                    .build();
            try {
                s3.deleteObjects(deleteObjectRequest);
                System.out.println("Batch deleting successfully");
            } catch (S3Exception e) {
                System.err.println("Error deleting Objecs: " + e.getMessage());
            }
        }
    }

    public void deleteEmptyBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("Bucket with the name: \"" + bucketName + "\" was deleted");
        } catch (S3Exception e) {
            System.err.println("Error Deleting bucket: " + e.getMessage());
        }
    }

    public String uploadFile(String filePath, String bucketName) throws IOException {
        log.debug("thats what i get :  " + filePath);
        File file = new File(filePath);
        if (!file.exists()) {
            log.error("file not found: " + filePath);
            throw new FileNotFoundException("File Not Found: " + filePath);
        }
        String fileName = file.getName();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        s3.putObject(putObjectRequest, RequestBody.fromFile(file));

        return "s3://" + bucketName + "/" + fileName;
    }
    public String uploadFilePublic(String filePath, String bucketName) throws IOException {
        log.debug("thats what i get :  " + filePath);
        File file = new File(filePath);
        if (!file.exists()) {
            log.error("file not found: " + filePath);
            throw new FileNotFoundException("File Not Found: " + filePath);
        }
        String fileName = file.getName();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .acl(ObjectCannedACL.PUBLIC_READ) // Optional: make the file public
                .build();

        s3.putObject(putObjectRequest, RequestBody.fromFile(file));

        return "s3://" + bucketName + "/" + fileName;
    }
    public String uploadFile(File file, String bucketName) throws IOException {
        log.debug("thats what i get path  :  " + file.getPath() + " name " + file.getName() + " absulote "
                + file.getAbsolutePath());

        // File file = new File(filePath);
        if (!file.exists()) {
            log.error("file not found: " + file.getPath());
            throw new FileNotFoundException("File Not Found: " + file.getPath());
        }
        String fileName = file.getName();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        s3.putObject(putObjectRequest, RequestBody.fromFile(file));

        return "s3://" + bucketName + "/" + fileName;
    }
public String generatePresignedUrl(String bucket, String key) {
    S3Presigner presigner = S3Presigner.builder()
            .region(region1)
            .build();

    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();

    PresignedGetObjectRequest presigned = presigner.presignGetObject(
            GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofHours(1))
                    .getObjectRequest(getObjectRequest)
                    .build()
    );

    return presigned.url().toString();
}

    public void download_file_from_S3(String fileUrl, File outputFile) throws Exception {
        log.debug("download_file_from_S3 start...");
        log.info("download_file_from_S3 parmeter: fileurl " + fileUrl + " output file " + outputFile);
        // Validate the S3 URL
        if (!fileUrl.startsWith("s3://")) {
            throw new IllegalArgumentException(
                    "Invalid S3 URL format. Expected format: s3://<bucket-name>/<object-key>");
        }

        // Extract bucket name and object key
        String[] s3Parts = fileUrl.replace("s3://", "").split("/", 2);
        if (s3Parts.length < 2) {
            throw new IllegalArgumentException("Invalid S3 URL format. Must include both bucket name and object key.");
        }
        String bucketName = s3Parts[0];
        String objectKey = s3Parts[1];
        log.info("download_file_from_S3 filed that used bucket name : " + bucketName + " and key " + objectKey);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            OutputStream os = new FileOutputStream(outputFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Download the file from S3 and save it locally
        // downloadFile(outputFile.toPath(), objectKey, bucketName);
    }

    public void downloadFile(Path localFilePath, String objectKey, String bucketName) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);

        try (FileOutputStream outputStreamFile = new FileOutputStream(localFilePath.toFile())) {
            outputStreamFile.write(objectBytes.asByteArray());
        }

        System.out.println("File downloaded to: " + localFilePath.toString());
    }

    public String getBucketUrl(String bucketName) {
        return "https://" + bucketName + ".s3." + region1.id() + ".amazonaws.com";
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        String queueUrl = null;
        queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        System.out.println("Queue URL is: " + queueName);
        return queueUrl;
    }

    /*
     * public void createSqsQueue(String queueName) {
     * CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
     * .queueName(queueName)
     * .build();
     * sqs.createQueue(createQueueRequest);
     * }
     */
    public String createQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        CreateQueueResponse createQueueResponse = null;
        createQueueResponse = sqs.createQueue(createQueueRequest);

        System.out.println("get to create queue with " + queueName);
        assert createQueueResponse != null;
        String queueUrl = createQueueResponse.queueUrl();
        System.out.println("Queue Created :\"" + queueName + "\" , with URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        sqs.deleteQueue(deleteQueueRequest);
    }

    /**
     * a function to check the size of the queue qwith this url
     *
     * @param queueName the url for the queue to check
     * @return the size of the queue
     */
    public int getQueueSize(String queueName) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueName)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)
                .build();
        GetQueueAttributesResponse queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                + Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED))
                + Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    /**
     * THIS FUNCTION TAKE FILE URL AND SPLIT IT INTO BUCKET NAME AND OBJECT KEY AND
     * IF THE FILE FROM THE RIGHT FORMAT
     * "s3://<bucket-name>/<object-key>" ITS DOWNLOADS THE FILE AND ADD IT TO THE
     * FILE'S HASH
     *
     * @param fileUrl : The S3 URL of the file
     * @param id      : The identifier (e.g. application ID) to use as the key in
     *                the hash map
     * @param hash    : The hash map where the downloaded File will be stored (key =
     *                id, value = File)
     * @throws Exception if the URL format is invalid or the download fails
     */
    public void downloadAndAddToHash(String fileUrl, String id, HashMap<String, File> hash) throws Exception {
        if (!fileUrl.startsWith("s3://")) {
            throw new IllegalArgumentException("This s3 url is invalid");
        }
        String[] parts = fileUrl.replace("s3://", "").split("/", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("This s3 url is invalid");
        }
        String bucketName = parts[0];
        String objectKey = parts[1];

        Path local = Paths.get(System.getProperty("java.io.tmpdir"), id + "_" + objectKey);
        System.out.println("add to hash this file : " + local.toString() + " with this key  " + objectKey);
        downloadFile(local, objectKey, bucketName);

        hash.put(id, local.toFile());
    }

    /// /////////////////////////// EC2 /////////////////////////////////////
    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region1).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_MEDIUM)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public synchronized boolean is_boss_work() {
        // the filters i want to apply
        // Filter runFilter =
        // Filter.builder().name("instance-state-name").values("running").build();
        Filter bossFilter = Filter.builder().name("tag:Name").values(Work_Type.MANAGER.name().toString()).build();

        // System.out.println(Work_Type.MANAGER.name());
        // System.out.println("after ----");

        // request
        DescribeInstancesRequest instancesRequest = DescribeInstancesRequest.builder().filters(
                Filter.builder()
                        .name("image-id")
                        .values(ami)
                        .build(),
                Filter.builder()
                        .name("instance-state-name")
                        .values("running")
                        .build(),
                bossFilter).build();

        // response
        DescribeInstancesResponse instancesResponse = ec2.describeInstances(instancesRequest);

        // iterate
        for (Reservation reservation : instancesResponse.reservations()) {
            // System.out.println("....");
            if (!reservation.instances().isEmpty()) {
                return true; // We found at least one instance matching the filters (running and tagged
                // MANAGER)
            }
        }

        return false;

    }

    public List<Instance> getAllInstance() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .collect(Collectors.toList());
    }

    public int countRunningInstancesFromAmi(String ami) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("image-id")
                                .values(ami)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running")
                                .build())
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);

        // Count the instances
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .filter(instance -> ami.equals(instance.imageId()))
                .mapToInt(instance -> 1) // Map each matching instance to 1
                .sum(); // Sum up the instances
    }

    private Instance runInstance_worker(String jarFileName, String ami, Work_Type workerT) {
        try {
            // --- FIX 1: Get Credentials from SDK (IAM Role) ---
            DefaultCredentialsProvider provider = DefaultCredentialsProvider.create();
            AwsCredentials credentials = provider.resolveCredentials();
            String awsAccessKeyId = credentials.accessKeyId();
            String awsSecretAccessKey = credentials.secretAccessKey();
            // 2. FIX: Safely retrieve sessionToken by casting or using the correct type
            // check
            String awsSessionToken = null;
            try {
                if (credentials instanceof software.amazon.awssdk.auth.credentials.AwsSessionCredentials) {
                    // Cast to the type that contains the session token
                    awsSessionToken = ((software.amazon.awssdk.auth.credentials.AwsSessionCredentials) credentials)
                            .sessionToken();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.err.println("i started that now -: " + workerT.name());
            System.err.println("your access key  -: " + awsAccessKeyId);

            // --- FIX 2: Generate Worker/Manager specific script ---
            String appDir = workerT.name().toLowerCase() + "-app";
            String localJarName = workerT.name().toLowerCase() + ".jar";

            String userDataScript;
            if (workerT == Work_Type.MANAGER) {
                // Use the static method for Manager, which contains complex snapshot logic
                // Ensure this static method is updated to accept credentials if it's not
                // currently defined
                // (Assuming you move the contents of generateUserDataScript to
                // generateManagerUserDataScript)
                userDataScript = generateUserDataScript(awsAccessKeyId, awsSecretAccessKey, awsSessionToken);
            } else { // WORKER
                // Use a generalized method for Worker. We'll define this next.
                userDataScript = generateWorkerUserDataScript(
                        awsAccessKeyId,
                        awsSecretAccessKey,
                        awsSessionToken);
            }

            String encodedUserData = Base64.getEncoder()
                    .encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));
            AWS.getInstance().createEC2(userDataScript, workerT.name(), 1);
            return null;
            // RunInstancesRequest runRequest = RunInstancesRequest.builder()
            // .instanceType(InstanceType.T2_MICRO)
            // .imageId(ami)
            // .minCount(1)
            // .maxCount(1)
            // .keyName("vockey")
            // .userData(encodedUserData)
            // .tagSpecifications(
            // TagSpecification.builder()
            // .resourceType(ResourceType.INSTANCE)
            // .tags(Tag.builder()
            // .key("Name")
            // .value(workerT.name())
            // .build())
            // .build())
            // .build();
            //
            //
            // RunInstancesResponse response = ec2.runInstances(runRequest);
            //
            // // FIX 3: Ensure non-null instance is returned if API succeeded
            // Instance newInstance = response.instances().get(0);
            // if (newInstance == null) {
            // throw new RuntimeException("EC2 API returned null instance object.");
            // }
            //
            // System.out.println("Instance ID: " + newInstance.instanceId() + "response key
            // name "
            // + newInstance.keyName());
            // return newInstance;

        } catch (SdkClientException e) {
            System.err.println("Error reading credentials via SDK: " + e.getMessage());
            return null; // SDK credential failure means the IAM role is missing or invalid
        } catch (Exception e) {
            System.err.println("Error launching EC2 instance: " + e.getMessage());
            return null;
        }
    }
    // Inside AWS.java (Define this as a new method)

    private String generateWorkerUserDataScript(
            String accessKeyId,
            String secretAccessKey,
            String sessionToken) { // worker.jar

        // Constants for the Manager application
        final String APP_DIR = "/home/ec2-user/worker-app";
        final String JAR_NAME_S3 = workerJar;
        final String JAR_NAME_LOCAL = "worker.jar"; // Use a simple name locally
        final String LOG_FILE = APP_DIR + "/worker.log";
        final String BUCKET_NAME = "zaki-manager-bucket";

        // IMPORTANT: The session token must be exported if your credentials are
        // temporary.
        String tokenExport = (sessionToken != null && !sessionToken.isEmpty())
                ? "export AWS_SESSION_TOKEN=\"" + sessionToken + "\"\n"
                : "";

        return "#!/bin/bash\n" +
                "set -ex\n" +

                "# 1. Installation\n" +
                "sudo yum update -y\n" +
                "sudo yum install -y java-17-amazon-corretto awscli\n" +

                "# 2. Setup App Directory and Credentials\n" +
                "mkdir -p " + APP_DIR + "\n" +

                // Inject credentials into environment (since they are temporary anyway)
                "export AWS_ACCESS_KEY_ID=\"" + accessKeyId + "\"\n" +
                "export AWS_SECRET_ACCESS_KEY=\"" + secretAccessKey + "\"\n" +
                tokenExport +

                "# 3. Download worker JAR\n" +
                "aws s3 cp s3://" + BUCKET_NAME + "/" + JAR_NAME_S3 + " " + APP_DIR + "/" + JAR_NAME_LOCAL + "\n" +

                "# 4. Fix Permissions for ec2-user\n" +
                // Change ownership from root (which ran the script) to ec2-user
                "sudo chown -R ec2-user:ec2-user " + APP_DIR + "\n" +

                "# 5. Clean up Credentials\n" +
                "unset AWS_ACCESS_KEY_ID\n" +
                "unset AWS_SECRET_ACCESS_KEY\n" +
                "unset AWS_SESSION_TOKEN\n" +

                "# 6. Start Manager Application (as ec2-user)\n" +
                // Execute the application using 'su' to switch from root to ec2-user
                "sudo -u ec2-user nohup java -jar " + APP_DIR + "/" + JAR_NAME_LOCAL + " >" + LOG_FILE + " 2>&1 &\n" +

                "# 7. Periodic Snapshot Uploader (runs as root to avoid permission issues with cron-like services)\n" +
                "(\n" +
                "  while true; do\n" +
                "    sleep 120\n" +
                "    SNAPSHOT=\"worker-$(date +%Y%m%d-%H%M%S).log\"\n" +
                "    # Copy log file *before* uploading to ensure it's readable by root\n" +
                "    sudo cp " + LOG_FILE + " /tmp/\"$SNAPSHOT\"\n" +
                "    aws s3 cp /tmp/\"$SNAPSHOT\" s3://" + BUCKET_NAME + "/worker_debug/\"$SNAPSHOT\" || true\n" +
                "    rm -f /tmp/\"$SNAPSHOT\"\n" +
                "  done\n" +
                ") &\n";
    }

    private Instance runInstance(String jarFileName, String ami, Work_Type workerT) {

        try {
            if (Work_Type.WORKER.name().equals(workerT.name())) {
                return runInstance_worker(jarFileName, ami, workerT);
            }
            // Read AWS credentials from ~/.aws/credentials
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            List<String> credentialsLines = Files.readAllLines(credentialsPath);

            // Extract access key, secret key, and session token
            String awsAccessKeyId = null;
            String awsSecretAccessKey = null;
            String awsSessionToken = null;

            for (String line : credentialsLines) {
                if (line.startsWith("aws_access_key_id")) {
                    awsAccessKeyId = line.split("=")[1].trim();
                } else if (line.startsWith("aws_secret_access_key")) {
                    awsSecretAccessKey = line.split("=")[1].trim();
                } else if (line.startsWith("aws_session_token")) {
                    awsSessionToken = line.split("=")[1].trim();
                }
            }

            if (awsAccessKeyId == null || awsSecretAccessKey == null || awsSessionToken == null) {
                throw new RuntimeException("Incomplete credentials in ~/.aws/credentials");
            }

            System.err.println("i started that now -: " + workerT.name());
            System.err.println("your access key  -: " + awsAccessKeyId);
            String script = generateUserDataScript(awsAccessKeyId, awsSecretAccessKey, awsSessionToken);
            String ins = AWS.getInstance().createEC2(script, workerT.name(), 1);
            new s3_debuge(getS3Client(), bucketName, "logs/app-log.txt").log("AWS",
                    "started instance " + ins + " of type " + workerT.name());
            return null;

            // ⬇️ START OF CORRECTED LOGIC ⬇️
            // String UserDataScript;
            // String appDir = workerT.name().toLowerCase() + "-app"; // e.g., "manager-app"
            // or "worker-app"
            // String localJarName = workerT.name().toLowerCase() + ".jar"; // e.g.,
            // "manager.jar" or "worker.jar"
            // // The region1 variable is correctly defined as static in the AWS class.

            // UserDataScript = generateScriptWithCredentials(
            // awsAccessKeyId,
            // awsSecretAccessKey,
            // awsSessionToken,
            // region1.toString(), // Use the static region1 variable
            // bucketName, // Use the instance variable bucketName
            // jarFileName, // Use the JAR file name passed to runInstance
            // appDir,
            // localJarName
            // );
            // // ⬆️ END OF CORRECTED LOGIC ⬆️
            //
            // // String userData =
            // Base64.getEncoder().encodeToString(userDataScript.getBytes());
            // String encodedUserData =
            // Base64.getEncoder().encodeToString(UserDataScript.getBytes(StandardCharsets.UTF_8));
            //
            // // return Base64.getEncoder().encodeToString(script.getBytes());
            // RunInstancesRequest runRequest = RunInstancesRequest.builder()
            // .instanceType(InstanceType.T2_MICRO)
            // .imageId(ami)
            // .minCount(1)
            // .maxCount(1)
            // .keyName("vockey")
            // .userData(encodedUserData)
            // .tagSpecifications(
            // TagSpecification.builder()
            // .resourceType(ResourceType.INSTANCE)
            // .tags(Tag.builder()
            // .key("Name")
            // .value(workerT.name())
            // .build())
            // .build())
            // .build();
            //
            //
            // RunInstancesResponse response = ec2.runInstances(runRequest);
            //
            // System.out.println("Instance ID: " + response.instances().get(0).instanceId()
            // + "response key name "
            // + response.instances().get(0).keyName());
            // return response.instances().get(0);
        } catch (IOException e) {
            System.err.println("Error reading credentials file: " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("Error launching EC2 instance: " + e.getMessage());
            return null;
        }
    }

    private static String generateUserDataScript(String accessKeyId,
            String secretAccessKey,
            String sessionToken) {

        // Constants for the Manager application
        final String APP_DIR = "/home/ec2-user/manager-app";
        final String JAR_NAME_S3 = "manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
        final String JAR_NAME_LOCAL = "manager.jar"; // Use a simple name locally
        final String LOG_FILE = APP_DIR + "/manager.log";
        final String BUCKET_NAME = "zaki-manager-bucket";

        // IMPORTANT: The session token must be exported if your credentials are
        // temporary.
        String tokenExport = (sessionToken != null && !sessionToken.isEmpty())
                ? "export AWS_SESSION_TOKEN=\"" + sessionToken + "\"\n"
                : "";

        return "#!/bin/bash\n" +
                "set -ex\n" +

                "# 1. Installation\n" +
                "sudo yum update -y\n" +
                "sudo yum install -y java-17-amazon-corretto awscli\n" +

                "# 2. Setup App Directory and Credentials\n" +
                "mkdir -p " + APP_DIR + "\n" +

                // Inject credentials into environment (since they are temporary anyway)
                "export AWS_ACCESS_KEY_ID=\"" + accessKeyId + "\"\n" +
                "export AWS_SECRET_ACCESS_KEY=\"" + secretAccessKey + "\"\n" +
                tokenExport +

                "# 3. Download Manager JAR\n" +
                "aws s3 cp s3://" + BUCKET_NAME + "/" + JAR_NAME_S3 + " " + APP_DIR + "/" + JAR_NAME_LOCAL + "\n" +

                "# 4. Fix Permissions for ec2-user\n" +
                // Change ownership from root (which ran the script) to ec2-user
                "sudo chown -R ec2-user:ec2-user " + APP_DIR + "\n" +

                "# 5. Clean up Credentials\n" +
                "unset AWS_ACCESS_KEY_ID\n" +
                "unset AWS_SECRET_ACCESS_KEY\n" +
                "unset AWS_SESSION_TOKEN\n" +

                "# 6. Start Manager Application (as ec2-user)\n" +
                // Execute the application using 'su' to switch from root to ec2-user
                "sudo -u ec2-user nohup java -jar " + APP_DIR + "/" + JAR_NAME_LOCAL + " >" + LOG_FILE + " 2>&1 &\n" +

                "# 7. Periodic Snapshot Uploader (runs as root to avoid permission issues with cron-like services)\n" +
                "(\n" +
                "  while true; do\n" +
                "    sleep 120\n" +
                "    SNAPSHOT=\"manager-$(date +%Y%m%d-%H%M%S).log\"\n" +
                "    # Copy log file *before* uploading to ensure it's readable by root\n" +
                "    sudo cp " + LOG_FILE + " /tmp/\"$SNAPSHOT\"\n" +
                "    aws s3 cp /tmp/\"$SNAPSHOT\" s3://" + BUCKET_NAME + "/manager_debug/\"$SNAPSHOT\" || true\n" +
                "    rm -f /tmp/\"$SNAPSHOT\"\n" +
                "  done\n" +
                ") &\n";
    }

    private String generateScriptWithCredentials1(
            String accessKeyId,
            String secretAccessKey,
            String sessionToken,
            String region,
            String bucket,
            String zipFileNameInS3, // file name inside S3 (zip or jar)
            String appDir,
            String finalJarName // jar file name AFTER unzip
    ) {

        // Add token only if it exists
        String tokenExport = (sessionToken != null && !sessionToken.isEmpty())
                ? "export AWS_SESSION_TOKEN=\"" + sessionToken + "\"\n"
                : "";

        return "#!/bin/bash\n" +
                "echo \"===== BOOTSTRAP STARTED =====\"\n" +

                "sudo yum update -y\n" +
                "sudo yum install -y java-17-amazon-corretto awscli unzip\n" +

                "echo \"Setting temporary AWS credentials...\"\n" +
                "export AWS_ACCESS_KEY_ID=\"" + accessKeyId + "\"\n" +
                "export AWS_SECRET_ACCESS_KEY=\"" + secretAccessKey + "\"\n" +
                "export AWS_DEFAULT_REGION=\"" + region + "\"\n" +
                tokenExport +

                "mkdir -p /home/ec2-user/" + appDir + "\n" +
                "cd /home/ec2-user/" + appDir + "\n" +

                "echo \"Downloading file from S3: " + zipFileNameInS3 + "\"\n" +
                "aws s3 cp s3://" + bucket + "/" + zipFileNameInS3 + " ./\n" +

                "if [[ \"" + zipFileNameInS3 + "\" == *.zip ]]; then\n" +
                "   echo \"Unzipping secured ZIP...\"\n" +
                "   unzip -P Aseel " + zipFileNameInS3 + "\n" +
                "else\n" +
                "   echo \"File is not ZIP → no unzip needed\"\n" +
                "fi\n" +

                "echo \"Fixing permissions...\"\n" +
                "sudo chown -R ec2-user:ec2-user /home/ec2-user/" + appDir + "\n" +

                "echo \"Cleaning temporary AWS credentials...\"\n" +
                "unset AWS_ACCESS_KEY_ID\n" +
                "unset AWS_SECRET_ACCESS_KEY\n" +
                "unset AWS_DEFAULT_REGION\n" +
                "unset AWS_SESSION_TOKEN\n" +

                "echo \"Starting application: " + finalJarName + "\"\n" +
                "nohup java -jar /home/ec2-user/" + appDir + "/" + finalJarName +
                " > /home/ec2-user/" + appDir + "/log.txt 2>&1 &\n" +

                "echo \"===== BOOTSTRAP COMPLETED SUCCESSFULLY =====\"\n";
    }

    // @return: truew if success with make worker
    // else return false (check if not up of the limit )
    public List<Instance> start_worker(String ami, int worker_number, Work_Type work_type) {

        int currentWorkers = 0;
        try {
            // compute the instance
            int managerCount = countRunningInstances(ami, Work_Type.MANAGER.name());
            currentWorkers = countRunningInstances(ami, Work_Type.WORKER.name());
            int totalRunningInstances = managerCount + currentWorkers;

            // compute number of working can add
            int availableSlots = MAX_TOTAL_INSTANCES - totalRunningInstances;
            int workersToLaunch = Math.min(worker_number, availableSlots);

            if (workersToLaunch <= 0) {
                if (currentWorkers + 1 >= MAX_TOTAL_INSTANCES) {
                    System.out.println("ERROR: Cannot launch more workers. Reached max limit of " + MAX_TOTAL_INSTANCES
                            + " instances.");
                }
                return null;
            }

            System.out.println("Launching " + workersToLaunch + " new worker instances...");

            List<Instance> launchedInstances = new ArrayList<>();

            if (Work_Type.MANAGER.name().equals(work_type.name()) & worker_number > 1) {
                System.out.println("are you mogol ?");
                System.err.println("ERROR : MANAGER SHOULD BE ONLY ONE !!!!");

            }
            for (int i = 0; i < workersToLaunch; i++) {
                log.debug("Starting instance " + (i + 1) + " of " + workersToLaunch);
                Instance newInstance = null;
                if (Work_Type.WORKER.name().equals(work_type.name())) {// worker
                    newInstance = runInstance(workerJar, ami, work_type);
                } else {// manager

                    newInstance = runInstance(managerJar, ami, work_type);
                }
                launchedInstances.add(newInstance);
            }

            System.out.println("Successfully launched " + launchedInstances.size() + " worker instances.");

            return launchedInstances;

        } catch (

        Exception e) {
            System.err.println("Error during worker startup: " + e.getMessage());
            return null;
        }
    }

    /**
     *
     * @param amiId
     * @param tagName
     * @return number the running instance you have
     */
    public int countRunningInstances(String amiId, String tagName) {
        int count = 0;

        // פילטרים: מצב 'running' ו-AMI ספציפי
        Filter runFilter = Filter.builder().name("instance-state-name").values("running").build();
        Filter amiFilter = Filter.builder().name("image-id").values(amiId).build();

        // פילטר תג: מחפש תג Name עם הערך Manager/Worker
        Filter tagFilter = Filter.builder().name("tag:Name").values(tagName).build();

        DescribeInstancesRequest instancesRequest = DescribeInstancesRequest.builder()
                .filters(runFilter, amiFilter, tagFilter)
                .build();

        DescribeInstancesResponse instancesResponse = ec2.describeInstances(instancesRequest);

        for (Reservation reservation : instancesResponse.reservations()) {
            count += reservation.instances().size();
        }
        return count;
    }

    public synchronized boolean start_manager() {
        if (is_boss_work()) {
            throw new IllegalStateException("Manager is already running");
        }

        try {
            System.out.println("[INFO] Manager is not active. Launching Manager instance...");

            List<Instance> managerInstance = start_worker(
                    AWS.ami,
                    1,
                    Work_Type.MANAGER);
            if (managerInstance == null) {
                System.err.println("[ERROR] Manager instance is null. Not starting a new one.");
            }

            System.out.println("[SUCCESS] Manager started with Instance ID: ");// +
                                                                               // managerInstance.get(0).instanceId());
            // Manager man = new Manager();
            return true;

        } catch (Exception e) {
            System.err.println("[ERROR] Failed to start Manager instance: " + e.getMessage());
            return false;
        }
    }

    public void deleteInstanceByWorkType(int numberOfInstanceToDelete, String amiId, Work_Type instanceWorkType) {
        try {
            DescribeInstancesResponse describeInstancesResponse = ec2
                    .describeInstances(DescribeInstancesRequest.builder().build());
            List<Instance> runningInstances = describeInstancesResponse.reservations().stream()
                    .flatMap(reserv -> reserv.instances().stream())
                    .filter(instance -> instance.state().name() == InstanceStateName.RUNNING)
                    .filter(instance -> instance.imageId().equals(amiId))
                    .filter(instance -> {
                        return instance.tags().stream().anyMatch(
                                tag -> tag.key().equals("Name") && tag.value().equals(instanceWorkType.name()));
                    }).collect(Collectors.toList());
            List<String> instanceIdToDelete = runningInstances.stream()
                    .limit(numberOfInstanceToDelete)
                    .map(Instance::instanceId)
                    .collect(Collectors.toList());
            if (!instanceIdToDelete.isEmpty()) {
                TerminateInstancesRequest terminateInstancesRequest = TerminateInstancesRequest.builder()
                        .instanceIds(instanceIdToDelete)
                        .build();
                ec2.terminateInstances(terminateInstancesRequest);
                System.out.println("Deleted Instances with work type " + instanceWorkType + " from AMI " + amiId + ": "
                        + instanceIdToDelete);
            } else {
                System.out.println("No instances to delete");
            }
        } catch (Ec2Exception e) {
            System.err.println("Error while deleting instances: " + e.awsErrorDetails().errorMessage());
        }

    }

    public boolean terminate() {
        List<String> instanceIdsToTerminate = new ArrayList<>();

        try {

            // get all the running manager instance
            List<Instance> managers = getRunningInstances(AWS.ami, Work_Type.MANAGER.name());
            managers.stream().map(Instance::instanceId).forEach(instanceIdsToTerminate::add);

            // get all running worker
            List<Instance> workers = getRunningInstances(AWS.ami, Work_Type.WORKER.name());
            workers.stream().map(Instance::instanceId).forEach(instanceIdsToTerminate::add);

            if (instanceIdsToTerminate.isEmpty()) {
                System.out.println("[INFO] No running instances found to terminate.");
                return true;
            } else {
                System.out.println("[INFO] there running instances ");
            }

            // terminated......
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceIdsToTerminate)
                    .build();

            ec2.terminateInstances(terminateRequest);

            // success message
            System.out.println("[SUCCESS] Terminated instances: " + instanceIdsToTerminate);

            return true;

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] Failed to terminate instances: " + e.getMessage());
            return false;
        }
    }

    public List<Instance> getRunningInstances(String amiId, String tagName) {
        Filter runFilter = Filter.builder().name("instance-state-name").values("running").build();
        Filter tagFilter = Filter.builder().name("tag:Name").values(tagName).build();
        Filter amiFilter = Filter.builder().name("image-id").values(amiId).build();

        DescribeInstancesRequest instancesRequest = DescribeInstancesRequest.builder()
                .filters(runFilter, tagFilter, amiFilter)
                .build();

        DescribeInstancesResponse instancesResponse = ec2.describeInstances(instancesRequest);

        return instancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .collect(Collectors.toList());
    }

    // SQS
    public void send_msg(String queueUrl, String msg) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(sendMessageRequest);

    }

    public List<Message> recive_msg(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(5)
                .build();

        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
        return messages;
    }

    public List<Message> recive_msg_form_sqs(String queueUrl) {
        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(5)
                .build();
        log.debug("recive the message " + queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

        if (messages != null)
            log.debug("the message " + messages.toString());
        else
            log.debug("the message from url " + queueUrl + " is null");
        return messages;
    }

    public boolean Queue_exist(String queue_name) {
        try {
            // Try to get the queue URL by its name
            String queueUrl = getQueueUrl(queue_name);
            // If no exception is thrown, the queue exists
            return true;
        } catch (QueueDoesNotExistException e) {
            // If a QueueDoesNotExistException is thrown, the queue does not exist
            return false;
        }

    }

    public void deleteMsgFromQueue(String queue, String receiptHandle) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queue)
                .receiptHandle(receiptHandle)
                .build());

    }

    public void deleteMessage(Message msg, String queueUrl) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
        System.out.println("The message was deleted");
    }

    /**
     * send a msg to queue
     *
     * @param queue_name the name of the queue to send in it
     * @param msg        message to send
     */
    public void send_msg_queue_name(String queue_name, String msg) {
        send_msg(getQueueUrl(queue_name), msg);
    }

    /**
     * recive the msg from the queue
     *
     * @param queue_name the queue name to recive message from it
     * @return return the message the recived
     */
    public List<Message> recive_msg_form_sqs_queue_name(String queue_name) {
        return recive_msg_form_sqs(getQueueUrl(queue_name));
    }

    /**
     * delete the message from the queue
     *
     * @param queue_name        the queue name that have the message
     * @param message_to_delete the message to delete
     */
    public void delete_msg_from_queue(String queue_name, Message message_to_delete) {

        String queueUrl = getQueueUrl(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message_to_delete.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
        log.info("the message was successfully deleted  ...");

        // Return the list of messages
    }

    /**
     * check if there exist object with key in the bucket
     * 
     * @param bucket to check on
     * @param key    of the object to check
     * @return true if and only if there exist object
     */
    public boolean s3ObjectExists(String bucket, String key) {
        try {
            s3.headObject(b -> b.bucket(bucket).key(key));
            return true;
        } catch (NoSuchKeyException e) {
            log.info("there no  such object with name " + key);
            return false;
        }
    }

    public S3Client getS3Client() {
        return s3;
    }
}
