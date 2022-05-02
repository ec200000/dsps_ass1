import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import java.io.OutputStream;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class Manager {
    private static AtomicBoolean active = new AtomicBoolean(true);
    static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    private static AtomicInteger workers = new AtomicInteger(0);
    private static final Object _lock = new Object();
    static ReentrantLock lock = new ReentrantLock();

    private static String downloadFile(String bucket_name, String key_name) {
        try{
//            System.out.println("d1");
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
//            System.out.println("d2");
            return res.asUtf8String();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    private static Message getMessage(String queueUrl){
        try{
//            System.out.println("gM1");
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
            while (messages.size()==0)
                messages = AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
//            System.out.println("gM2");
            return messages.get(0);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    private static void createBatches(LinkedList<SendMessageBatchRequestEntry> messages, String queueUrl){
        for(int i=0; i<messages.size(); i = i+10){
            List<SendMessageBatchRequestEntry> entries = messages.subList(i,i+9);
            AWSManager.getInstance().sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build());
        }

    }

    private static int createMessages(String data, String queueUrl, String appID) {
        try{
//            System.out.println("cM1");
            LinkedList<SendMessageBatchRequestEntry> entries = new LinkedList<>();
            int messages = 0;
            for (String d: data.split("\n")) {
                String[] d2 = d.split("\t");
                String body = appID+";"+d2[1]+"^"+d2[0];
                SendMessageBatchRequestEntry entry = SendMessageBatchRequestEntry.builder()
                        .messageBody(body)
                        .messageGroupId("group1"+messages)
                        .messageDeduplicationId(""+messages+System.currentTimeMillis())
                        .id(""+messages+System.currentTimeMillis())
                        .build();
                entries.add(entry);
                if(entries.size() % 10 == 0){
                    AWSManager.getInstance().sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                            .queueUrl(queueUrl)
                            .entries(entries)
                            .build());
                    entries = new LinkedList<>();
                }

                messages++;
            }
//            System.out.println("cM2");
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return -1;
    }

    //   start APPID;start$n#bucket^key
    private static String[] parseStartMessage(String msg) {
        String appid = msg.split(";")[0];
        String n = msg.split("\\$")[1].split("#")[0];
        String bucket = msg.split("#")[1].split("\\^")[0];
        String key = msg.split("\\^")[1];
        String[] res = new String[4];
        res[0] = appid;
        res[1] = n;
        res[2] = bucket;
        res[3] = key;
        return res;
    }

    //   terminate APPID;terminate
    private static String parseTerminateMessage(String msg) {
        return msg.split(";")[0];
    }

    //  finished: APPID;done$link#action@bucket^key
    private static String[] parseDoneMessage(String msg) {
        String appid = msg.split(";")[0];
        String link = msg.split("$")[1].split("#")[0];
        String action = msg.split("#")[1].split("@")[0];
        String bucket = msg.split("@")[1].split("^")[0];
        String key = msg.split("^")[1];
        String[] res = new String[5];
        res[0] = appid;
        res[1] = link;
        res[2] = action;
        res[3] = bucket;
        res[4] = key;
        return res;
    }

    private static LinkedList<Message> getFinishedTasks(String queueUrl, String appID){
        try{
//            System.out.println("gFT1");
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .visibilityTimeout(10)
                    .build();
            List<Message> allMessages = AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
            LinkedList<Message> messages = new LinkedList<>();
            for(Message m : allMessages)
                if(m.body().split(";")[0].contains(appID)){
                    messages.add(m);
                }
//            System.out.println("gFT2");
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    private static void deleteMessage(String queueUrl, Message message){
        try{
//            System.out.println("dM1");
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            AWSManager.getInstance().sqsClient.deleteMessage(deleteMessageRequest);
//            System.out.println("dM2");
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void sendDoneMessage(String queueUrl, String body){
        try{
//            System.out.println("sDM1");
            AWSManager.getInstance().sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(body)
                    .messageGroupId("group1")
                    .messageDeduplicationId(""+System.currentTimeMillis())
                    .build());
//            System.out.println("sDM2");
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void createWorkers(int numOfWorkers, String appId) {
        int activeWorkers = workers.get(); // get number of workers

        for (int i = activeWorkers; i < numOfWorkers - activeWorkers && i <= 18; i++) {
            String id = AWSManager.getInstance().createWorker("worker" + (i+1) + appId, "ami-00e95a9222311e8ed", "Worker");
//            System.out.println("6");
            workers.incrementAndGet(); // add worker
        }
    }

    public static void main(String[] args){
//        System.out.println("1");
        while(active.get()){
//            System.out.println("2");
            LinkedList<String> queues = AWSManager.getInstance().queues;
            String queueUrl = queues.get(0); // get messages from local applications
            String workersQueue = queues.get(1);
            String fromWorkersUrl = queues.get(2);

            Message m = getMessage(queueUrl); // takes message
            if(m.body().contains("terminate")){
                active.set(false); // time to finish
                deleteMessage(queueUrl, m); // delete terminate message
                AWSManager.getInstance().deleteSQSQueue("LocalToManager.fifo"); // delete queue, no new locals
                break;
            }
//            System.out.println("3");
            deleteMessage(queueUrl, m);

            Runnable newLocalApp = new Runnable() {
                @Override
                public void run() {
//                    System.out.println("4");
                    String[] msg = parseStartMessage(m.body());
                    String bucket = msg[2];
                    String key = msg[3];
                    int n = Integer.parseInt(msg[1]);
                    String appId = msg[0];

                    String localQueue = AWSManager.getInstance().createSQS(appId); // create queue for local

                    String file = downloadFile(bucket, key); // get input file

                    int msgs = createMessages(file, workersQueue, appId); // create messages from input file
//                    System.out.println("5");
                    System.out.println(msgs);
                    int numOfWorkers = msgs/n;
                    if (msgs%n!=0) numOfWorkers++;
                    if (numOfWorkers > 18)
                        numOfWorkers = 18;

                    lock.lock();
                    try{
                        createWorkers(numOfWorkers, appId);
                    } finally {
                        lock.unlock();
                    }

                    // create linked list for messages from the workers. it's size is the messages number.
                    // delete every message entered the list from the queue

                    LinkedList<Message> messages = new LinkedList<>();
//                    System.out.println("msgs = "+msgs);
                    while(messages.size() < msgs){
                        LinkedList<Message> finishedTasks = getFinishedTasks(fromWorkersUrl, appId);
//                        System.out.println("7");
                        if (finishedTasks!=null && finishedTasks.size() > 0) {
                            messages.addAll(finishedTasks);
                            for(int i=0; i< finishedTasks.size(); i++){
                                deleteMessage(fromWorkersUrl, finishedTasks.get(i));
                            }
                        }
//                        System.out.println("messages size = "+messages.size());
                    }
//                    System.out.println("8");

                    FileWriter writer = null;
                    try {
                        writer = new FileWriter(appId+".txt"); // create text file
                        for (Message mesg : messages) {
                            writer.write(mesg.body() + System.lineSeparator()); // enter results to file
                        }
                        writer.close();
                    }   catch (IOException e) {
                            System.err.println(e.getMessage());
                    }
                    String link = AWSManager.getInstance().uploadFile(appId+".txt", "output");
                    String finalMsg = appId+";done$"+link;
                    sendDoneMessage(localQueue, finalMsg);
//                    System.out.println("9");
                }
            };
//            System.out.println("executing");
            executor.execute(newLocalApp); // add thread to pool
        }

        System.out.println("11"); // got terminate

        while (executor.getTaskCount() != executor.getCompletedTaskCount()){

        }

        executor.shutdown(); // waiting for threads to finish

        AWSManager.getInstance().deleteSQSQueue("ManagerToWorker.fifo"); // tell workers to finish

        System.out.println("13");
        String id = AWSManager.getInstance().getInstance("manager");
        if (!id.equals(""))
            AWSManager.getInstance().stopInstance(id);
        return;
    }
}