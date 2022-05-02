import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AWSManager {
    Region region = Region.US_EAST_1;
    Ec2Client ec2 = Ec2Client.builder().region(region).build();
    Region region2 = Region.US_WEST_2;
    SqsClient sqsClient = SqsClient.builder().region(region2).build();
    S3Client s3 = S3Client.builder().region(region2).build();
    LinkedList<String> queues = startQueues();

    private static class SingletonHolder {
        private static AWSManager instance = new AWSManager();
    }
    private AWSManager() {
        // initialization code..
    }
    public static AWSManager getInstance() {
        return SingletonHolder.instance;
    }

    public LinkedList<String> startQueues(){
        LinkedList<String> queuesLinks = new LinkedList<>();
        queuesLinks.add(createSQS("LocalToManager"));
        queuesLinks.add(createSQS("ManagerToWorker"));
        queuesLinks.add(createSQS("WorkerToManager"));
        return queuesLinks;
    }

    // Checks if the manager is running. yes => null, no => its id
    public String getManager() {
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if(instance != null && instance.tags().size()>0 && instance.tags().get(0).value().equals("manager")){
                            if(instance.state().name() == InstanceStateName.TERMINATED || instance.state().name() == InstanceStateName.SHUTTING_DOWN)
                                return "";
                            if(instance.state().name() == InstanceStateName.RUNNING || instance.state().name() == InstanceStateName.PENDING)
                                return null;
                            else
                                return instance.instanceId();
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return "";

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    // Checks if the manager is running. yes => null, no => its id
    public int getWorkers() {
        String nextToken = null;
        int workers = 0;

        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if(instance != null && instance.tags().size()>0 && instance.tags().get(0).value().contains("worker")){
                            if(instance.state().name() == InstanceStateName.RUNNING || instance.state().name() ==InstanceStateName.PENDING)
                                workers++;
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return workers;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return workers;
    }

    public List<String> listQueues(String prefix) {
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            return listQueuesResponse.queueUrls();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return new LinkedList<>();
    }

    public String createSQS(String queueName){
        try {
            Map<QueueAttributeName, String> queueAttributes = new HashMap<QueueAttributeName, String>();

            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");

            String queueUrl = sqsClient.createQueue(CreateQueueRequest.builder()
                            .queueName(queueName+".fifo")
                            .attributes(queueAttributes).build()).queueUrl();
            return queueUrl;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    public void stopInstance(String instanceId) {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.terminateInstances(request);
            System.out.printf("Successfully terminated instance %s", instanceId);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s", instanceId);
    }

    public String createManager(String name, String amiId, String job) {
        String userDataScript = "#! /bin/sh\n" +
                "sudo mkdir "+job+"Files\n" +
                "sudo aws s3 cp s3://dspsass1/JarFiles/"+job+".jar ./"+job+"Files\n" +
                "sudo java -jar /"+job+"Files/"+job+".jar\n";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MEDIUM)
                .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes(UTF_8)))
                .maxCount(1)
                .minCount(1)
                .securityGroupIds("sg-0943a0097bbd58df7")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::788820258204:instance-profile/LabInstanceProfile").build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);

            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        return "";
    }

    public synchronized String createWorker(String name, String amiId, String job) {
        String userDataScript = "#! /bin/sh\n" +
                "sudo mkdir "+job+"Files\n" +
                "sudo aws s3 cp s3://dspsass1/JarFiles/"+job+".jar ./"+job+"Files\n" +
                "sudo java -jar /"+job+"Files/"+job+".jar " + name + "\n";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes(UTF_8)))
                .maxCount(1)
                .minCount(1)
                .securityGroupIds("sg-0943a0097bbd58df7")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::788820258204:instance-profile/LabInstanceProfile").build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);

            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        return "";
    }

    public String createBucket(String bucket){
        bucket = bucket + System.currentTimeMillis();
        try{
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket)
                    .build());
            System.out.println("Creating bucket: " + bucket);
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            System.out.println(bucket +" is ready.");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return bucket;
    }

    public String uploadToBucket(String bucket, String inputFile){
        try{
            String key = "key" + System.currentTimeMillis();
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            File f = new File(inputFile);
            s3.putObject(objectRequest, RequestBody.fromFile(f));
            return bucket+"^"+key; // messages look like this: appid;bucket^key
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    public String uploadFile(String inputFile, String key){
        String bucket = "bucket" + System.currentTimeMillis();

        try{
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket)
                    .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region2.id()).build())
                    .build());
            System.out.println("Creating bucket: " + bucket);
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            System.out.println(bucket +" is ready.");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        File f = new File(inputFile);
        s3.putObject(objectRequest, RequestBody.fromFile(f));
        return bucket+"^"+key; // messages look like this: appid;bucket^key
    }

    public void deleteSQSQueue(String queueName) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueName)
                    .build();
            if(listQueues(queueName).size()>0)
                sqsClient.deleteQueue(deleteQueueRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public String getInstance(String name) {
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if(instance != null && instance.tags().size()>0 && instance.tags().get(0).value().equals(name)){
                            return instance.instanceId();
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return "";

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    public void deleteBucketObjects(String bucketName) {
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response listObjectsV2Response;
            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while(listObjectsV2Response.isTruncated());

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void deleteBuckets(){
        // List buckets
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
        List<Bucket> buckets = listBucketsResponse.buckets();
        for (Bucket b : buckets){
            if(!b.name().equals("dspsass1"))
                deleteBucketObjects(b.name());
        }
    }

    public static void main(String[] args) {
//        AWSManager.getInstance().deleteSQSQueue("LocalToManager.fifo");
//        AWSManager.getInstance().deleteSQSQueue("ManagerToWorker.fifo");
//        AWSManager.getInstance().deleteSQSQueue("WorkerToManager.fifo");
        AWSManager.getInstance().deleteBuckets();
    }
}
