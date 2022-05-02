import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.io.File;
import java.util.function.Predicate;

/*
* Message formats:
*   LocalApplication to Manager:
*       1. start APPID;start$n#bucket^key
*       2. terminate APPID;terminate
*   Manager to LocalApplication:
*       1. finished APPID;done$bucket^key
*
*   Manager to Worker:
*       1. start: APPID;link^action
*   Worker to Manager:
*       1. finished: APPID;done$link#action@bucket^key
*
* */

public class LocalApplication {
    static String inputFile;
    static String outputFile;
    static int n; // PDF files per worker
    static Boolean terminate = false;
    static long appID;
    static String managerID;

    public static void sendStartMessage(String queueUrl, String fileUrl){
        SendMessageResponse res = AWSManager.getInstance().sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(appID+";start$"+n+"#"+fileUrl)
                .messageDeduplicationId(""+System.currentTimeMillis())
                .messageGroupId("group1")
                .build());
    }

    public static void sendTerminateMessage(String queueUrl){
        try{
            AWSManager.getInstance().sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(appID+";terminate")
                    .messageDeduplicationId(""+System.currentTimeMillis())
                    .messageGroupId("group1")
                    .build());
        }catch (SqsException e) {
        }
    }

    public static List<Message> checkForMessage(String queueUrl){
        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(5)
                    .build();
            return AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

//    finished -> APPID;done$bucket^key
    private static String[] parseDoneMessage(String msg) {
        String appid = msg.split(";")[0];
        String bucket = msg.split("\\$")[1].split("\\^")[0];
        String key = msg.split("\\^")[1];
        String[] res = new String[3];
        res[0] = appid;
        res[1] = bucket;
        res[2] = key;
        return res;
    }

    private static String downloadFile(String key_name, String bucket_name) {
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder().region(region).build();
        boolean bucket_exists = false;
        while(!bucket_exists){
            ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
            ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
            Predicate<Bucket> containsName = i -> (i.name().equals(bucket_name));
            if(listBucketsResponse.buckets().stream().anyMatch(containsName))
                bucket_exists = true;
        }

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket_name)
                .key(key_name)
                .build();

        ResponseBytes<GetObjectResponse> res = s3.getObjectAsBytes(getObjectRequest);
        return res.asUtf8String();
    }

    public static void deleteSQSQueue(String queueName) {

        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueName)
                    .build();

            AWSManager.getInstance().sqsClient.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void deleteMessage(String queueUrl, Message message){
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        AWSManager.getInstance().sqsClient.deleteMessage(deleteMessageRequest);
    }

    public static void main(String[] args){
        appID = System.currentTimeMillis();
        System.out.println(appID);
        inputFile = args[0];
        outputFile = args[1];
        n = Integer.parseInt(args[2]);

        if(args.length == 4 && args[3].equals("terminate")){
            terminate = true;
        }

        String fileUrl = AWSManager.getInstance().uploadFile(inputFile, "input"); // upload input file

        LinkedList<String> queues = AWSManager.getInstance().queues;

        String queueUrl = queues.get(0);

        sendStartMessage(queueUrl, fileUrl); // send the url to the manager

        managerID = AWSManager.getInstance().getManager(); // does the manager exist?

        if(managerID != null && managerID.equals("")){
            managerID = AWSManager.getInstance().createManager("manager",
                    "ami-00e95a9222311e8ed", "Manager"); // create manager
        }

        List<String> managerQueues = AWSManager.getInstance().listQueues((String.valueOf(appID)));

        while(managerQueues.size() == 0) {
            managerQueues = AWSManager.getInstance().listQueues((String.valueOf(appID)));
        }

        String managerQueue = managerQueues.get(0);// get queue from manager

        List<Message> messages = new LinkedList<>();

        while(messages.size() == 0)
            messages = checkForMessage(managerQueue); // get messages

        String[] message = new String[0];

        for (Message m:messages) {
            if(m.body().contains("done") && m.body().contains((String.valueOf(appID)))){ // get summary file
                message = parseDoneMessage(m.body());
                deleteMessage(managerQueue, m);
                break;
            }
        }

        String bucket = message[1];
        String key = message[2];

        String file = downloadFile(key, bucket); // download the summary file
        String[] fileLines = file.split("\n");
        String[] newLines = new String[fileLines.length];

        int i = 0;
        for (String line: fileLines) {
            if (!line.contains("error")) {
                String bucket1 = line.split("\\*")[1].split("\\^")[0];
                String key1 =line.split("\\*")[1].split("\\^")[1];
                String newLine = "<" + line.split("#")[1].split("\\*")[0] + ">: " +
                        line.split("\\$")[1].split("#")[0] + " " +
                        "https://"+bucket1+".s3."+AWSManager.getInstance().region2.toString().toLowerCase()+".amazonaws.com/"+key1;
                        ;
                newLines[i] = newLine;
            }
            else {
                String newLine = "<" + line.split("#")[1].split("\\*")[0] + ">: " +
                        line.split("\\$")[1].split("#")[0] + " " +
                        "<" + line.split("\\*")[1] + ">";
                newLines[i] = newLine;
            }
            i++;
        }

        FileWriter fWriter = null;
        try {
            fWriter = new FileWriter(outputFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fWriter);
            bufferedWriter.write(PDFText2HTML.writeStringArr(newLines).toString()); // write to the html file
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        if (terminate) {
            System.out.println("terminating");
            sendTerminateMessage(queueUrl); // terminate manager
        }

        return;
    }
}