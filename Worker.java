import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;

public class Worker {

    static String queueUrl = "";
    static String appID = "";
    static String name = "";

    private static Message getMessage(String queueUrl){
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(60)
                    .build();
            List<Message> messages = AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
            while (messages.size()==0)
                messages = AWSManager.getInstance().sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages.get(0);
        } catch (SqsException e) {
//            System.out.println("queue dead");
            return Message.builder().body(e.awsErrorDetails().errorMessage()).build();
        }
    }

    private static boolean downloadPDF(String pdfUrl, String action){
        try {
            final URLConnection connection = new URL(pdfUrl).openConnection();
            connection.setConnectTimeout(15000);
            final FileOutputStream output = new FileOutputStream(new File("someFile.pdf"), false);
            final byte[] buffer = new byte[2048];
            int read;
            final InputStream input = connection.getInputStream();
            while((read = input.read(buffer)) > -1)
                output.write(buffer, 0, read);
            output.flush();
            output.close();
            input.close();
            return true;
        } catch (IOException e) {
            sendErrorMessage(pdfUrl, action, e.getMessage());
            return false;
        }
    }

    private static void convertFile(String bucket, String path, String action, String link){
        // path = filename on the local computer
        // action = the action
        // link = pdf url
        try {
            String file = "";
            switch (action) {
                case "ToImage":
                    PDFConvert.parseImage(path);
                    file = AWSManager.getInstance().uploadToBucket(bucket, "ass1.png");
                    break;
                case "ToHTML":
                    PDFConvert.parseHTML(path);
                    file = AWSManager.getInstance().uploadToBucket(bucket, "ass1.html");
                    break;
                case "ToText":
                    PDFConvert.parseText(path);
                    file = AWSManager.getInstance().uploadToBucket(bucket, "ass1.txt");
                    break;
            }
            sendDoneMessage(link, action, file); // pdfURL, action, s3 link
        }
        catch(IOException e){
            sendErrorMessage(link, action, e.getMessage());
        }
    }

    private static void sendDoneMessage(String path, String action, String fileUrl){
        try {
            AWSManager.getInstance().sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(appID+";done$"+path+"#"+action+"*"+fileUrl)
                    .messageGroupId("group1")
                    .messageDeduplicationId(""+name+System.currentTimeMillis())
                    .build());
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void sendErrorMessage(String path, String action, String error){
        try {
            AWSManager.getInstance().sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(appID+";error$"+path+"#"+action+"*"+error)
                    .messageGroupId("group1")
                    .messageDeduplicationId(""+name+System.currentTimeMillis())
                    .build());
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void deleteMessage(String queueUrl, Message message) {
        System.out.println("\nDelete Messages");
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            AWSManager.getInstance().sqsClient.deleteMessage(deleteMessageRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    //  start: APPID;link^action
    private static String[] parseStartMessage(String msg) {
        String appid = msg.split(";")[0];
        String link = msg.split(";")[1].split("\\^")[0];
        String action = msg.split("\\^")[1];
        String[] res = new String[3];
        res[0] = appid;
        res[1] = link;
        res[2] = action;
        return res;
    }

    public static void main(String[] args){
//        System.out.println("1");
        name = args[0];
        LinkedList<String> queues = AWSManager.getInstance().queues;
        String fromManager = queues.get(1);
        queueUrl = queues.get(2);
        String bucket = AWSManager.getInstance().createBucket(name);
        while(true){
            Message m = getMessage(fromManager);
//            System.out.println("2");
            if(m.body().contains("^")){
                String[] parse = parseStartMessage(m.body());
//                System.out.println("3");
                String link = parse[1];
                appID = parse[0];
                String action = parse[2];
                if(link.contains("\r"))
                    link = link.substring(0, link.length()-1);
//                System.out.println("4");
                boolean pdf = downloadPDF(link, action);
//                System.out.println("5");
                if(pdf) {
                    convertFile(bucket, "someFile.pdf", action, link);
//                    System.out.println("6");
                }
                deleteMessage(fromManager, m);
//                System.out.println("7");
            }
            else{
//                System.out.println("8");
                String id = AWSManager.getInstance().getInstance(name);
                if (!id.equals(""))
                    AWSManager.getInstance().stopInstance(id);
                return;
            }
        }
    }
}