package com.myproject;

import com.myproject.common.AWS;
import com.myproject.common.myLogger;
import com.myproject.common.s3_debuge;
import com.myproject.common.Work_Type;
import com.myproject.common.SpecialMessages;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

import javax.imageio.IIOException;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import static java.lang.Thread.sleep;

public class Manager {

    static myLogger log = new myLogger(Manager.class);
    final static AWS aws = AWS.getInstance();

    // private String bucketUrl;
    private String appManagerQueueUrl;
    private String managerToAppQueueUrl;
    private String threadManagerQueueUrl;
    private String threadWorkersQueueUrl;
    private String managerToWorkerQueueUrl;

    private final String bucketName = "zaki-manager-bucket";
    private String appToManagerQueueUrlName = "app-manager_queue";
    private String managerToAppQueueUrlName = "manager-app_queue";
    private String threadManagerQueueName = "thread_Manager_Queue_Name";
    private String threadWorkersQueueName = "thread_Worker_Queue_Name";
    private String managerToWorkerQueueName = "manager_To_Worker_Queue_Name";

    private boolean isGetTerminateMsg = false;
    private boolean isStopThreads = false;
    // private final Integer maxWorkers = 18;
    private Integer ActiveWorkerCount = 0;
    private Integer messagePerApp = 50;

    private final Object ActiveWorkerNumber = new Object();
    private final Object workingHashObject = new Object();

    private static HashMap<String, File> inputFileHash;
    private static HashMap<String, Integer> workHash;
    private static HashMap<String, File> outputFileHash;
    private static HashMap<String, Integer> IdToAddedWorkers;

    private Thread recThread;
    private Thread managerToWorkerQueueThread;
    private Thread workerToManagerThread;

    private Integer count = 0;

    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.start();
    }

    public void start() {
        System.out.println("Manager has started");

        aws.createBucketIfNotExists(bucketName);
        // bucketUrl = aws.getBucketUrl(bucketName);
        // Create if not exists, then get URL for ALL queues
        if (!aws.Queue_exist(appToManagerQueueUrlName)) {
            aws.createSqsQueue(appToManagerQueueUrlName);
        }
        appManagerQueueUrl = aws.getQueueUrl(appToManagerQueueUrlName);

        if (!aws.Queue_exist(managerToAppQueueUrlName)) {
            aws.createSqsQueue(managerToAppQueueUrlName);
        }
        managerToAppQueueUrl = aws.getQueueUrl(managerToAppQueueUrlName);

        if (!aws.Queue_exist(threadManagerQueueName)) {
            aws.createSqsQueue(threadManagerQueueName);
        }
        threadManagerQueueUrl = aws.getQueueUrl(threadManagerQueueName);

        if (!aws.Queue_exist(threadWorkersQueueName)) {
            aws.createSqsQueue(threadWorkersQueueName);
        }
        threadWorkersQueueUrl = aws.getQueueUrl(threadWorkersQueueName);

        if (!aws.Queue_exist(managerToWorkerQueueName)) {
            aws.createSqsQueue(managerToWorkerQueueName);
        }
        managerToWorkerQueueUrl = aws.getQueueUrl(managerToWorkerQueueName);
        // threadManagerQueueUrl = aws.createQueue(threadManagerQueueName);
        // threadWorkersQueueUrl = aws.createQueue(threadWorkersQueueName);
        // managerToWorkerQueueUrl = aws.createQueue(managerToWorkerQueueName);

        inputFileHash = new HashMap<>();
        outputFileHash = new HashMap<>();
        workHash = new HashMap<>();
        IdToAddedWorkers = new HashMap<>();

        workerToManagerThread = new Thread(() -> {
            try {
                try {
                    System.out.println("[MANAGER] [LESSENING TO WORKER MSG ]");
                    readMsgFromWorker();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        recThread = new Thread(() -> {
            try {
                recieveNewApp();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        managerToWorkerQueueThread = new Thread(() -> {
            try {
                try {
                    sendMsgToWorker();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        recThread.start();
        managerToWorkerQueueThread.start();
        workerToManagerThread.start();
    }

    private void readMsgFromWorker() throws Exception {
        while (true) {
            while (!isGetTerminateMsg && aws.getQueueSize(managerToWorkerQueueUrl) == 0) {
                sleep(10000);
            }
            List<Message> workerMessages = aws.recive_msg(managerToWorkerQueueUrl);
            for (Message message : workerMessages) {
                log.debug("[MANAGER GET A MSG] " + message.body());
                String body = message.body();
                String[] parts = parseMsg(body);
                String operation = parts[0];
                String inputFile = parts[1];
                String outputFile = parts[2];
                String appId = parts[3];
                String flag = parts[5];

                if (appId == null || appId.isEmpty()) {
                    log.error("Invalid appId in message: " + body);
                    aws.deleteMsgFromQueue(managerToWorkerQueueUrl, message.receiptHandle());
                    continue;
                }
                if (outputFileHash.containsKey(appId)) {
                    String newLine;

                    newLine = operation + "    " + inputFile + "   " + outputFile;
                    if (flag.compareTo("0") == 0 && (outputFile == null || outputFile.isEmpty())) {

                        newLine = operation + "    " + inputFile + "   " + " error in file-  is not available> ";
                    }
                    File summaryFile = outputFileHash.get(appId);// outputFileHash.put(appID, new File("outputFiles" +
                                                                 // appID));
                    // outputfiles+appid
                    addLineToFile(summaryFile, newLine);

                    count++;
                    System.out.println("This is the " + count + " message");
                    if (count == 10000) {
                        System.out.println("The counter reach 10000");
                        count = 0;
                        System.out.println("The counter reset to 0");
                    }
                    synchronized (workingHashObject) {
                        int val = workHash.get(appId);
                        workHash.remove(appId);
                        workHash.put(appId, val - 1);
                    }
                    if (isLastMessage(appId)) {
                        log.debug("uploading the output file  the output file path is "
                                + outputFileHash.get(appId).getPath() + " to S3 bucket: " + bucketName);
                        log.debug("the file is " + outputFileHash.get(appId));
                        String outPutFileUrl = aws.uploadFile(outputFileHash.get(appId).getPath(), bucketName);
                        // String queueUrl = aws.getQueueUrl(appId);
                        String queueUrl = aws.getQueueUrl(managerToAppQueueUrlName);
                        aws.send_msg(queueUrl, outPutFileUrl);
                        System.out.println("Message was sent to application");
                        File deleteFile = outputFileHash.remove(appId);
                        deleteFile.delete();
                        workHash.remove(appId);
                        synchronized (ActiveWorkerNumber) {
                            int addedWorkers = IdToAddedWorkers.get(appId);
                            aws.deleteInstanceByWorkType(addedWorkers, aws.ami, Work_Type.WORKER);
                            ActiveWorkerCount = ActiveWorkerCount - IdToAddedWorkers.get(appId);
                            IdToAddedWorkers.remove(appId);
                        }
                    }
                }
                try {
                    aws.deleteMsgFromQueue(managerToWorkerQueueUrl, message.receiptHandle());
                    System.out.println("Message deleted from manager_worker queue");
                } catch (SqsException e) {
                    System.err.println("Error while deleting message: " + e.awsErrorDetails().errorMessage());
                }
            }
            if (isGetTerminateMsg && inputFileHash.isEmpty() && outputFileHash.isEmpty()) {
                isStopThreads = true;
                System.out.println("Threads stop. Start cleaning up");
                Thread cleanUpThread = new Thread(() -> cleanup());
                cleanUpThread.start();
                break;
            }
        }
    }

    private void recieveNewApp() throws Exception {
        while (!isGetTerminateMsg) {
            while (aws.getQueueSize(appManagerQueueUrl) == 0) {
                sleep(20000);
            }

            List<Message> messages = aws.recive_msg(appManagerQueueUrl);

            for (Message message : messages) {
                String body = message.body();
                log.debug("Received message: " + body);

                if (body.equals(SpecialMessages.TERMINATE.toString())) {
                    log.debug("Manager Recieve Terminate Massage");
                    isGetTerminateMsg = true;
                } else {
                    String[] parts = parseMsg(body);
                    String inputPath = parts[0];
                    String appID = parts[1];
                    int n = Integer.parseInt(parts[2]);

                    aws.downloadAndAddToHash(inputPath, appID, inputFileHash);

                    File inputFile = inputFileHash.get(appID);
                    int totalLines = getNumOfLine(inputFile);

                    int increasedWorkers = (totalLines + n - 1) / n;

                    synchronized (ActiveWorkerNumber) {
                        log.debug("Calculating workers to launch for appID: " + appID);
                        int canLaunch = Math.min(increasedWorkers,
                                aws.MAX_TOTAL_INSTANCES - ActiveWorkerCount);

                        if (canLaunch > 0) {
                            aws.start_worker(aws.ami, canLaunch, Work_Type.WORKER);
                            ActiveWorkerCount += canLaunch;
                        }

                        IdToAddedWorkers.put(appID, canLaunch);
                        log.debug("Workers to launch for appID " + appID + ": " + canLaunch);
                    }
                    log.debug("Preparing output file for appID: " + appID);
                    File testOutFile = new File("/home/ec2-user/manager_outputs", "output_" + appID + ".txt");
                    testOutFile.getParentFile().mkdirs();
                    try {
                        if (testOutFile.getParentFile() != null && !testOutFile.getParentFile().exists()) {
                            testOutFile.getParentFile().mkdirs();
                        }
                        if (!testOutFile.exists()) {
                            testOutFile.createNewFile();
                        }
                    } catch (Exception e) {
                        log.error("Failed to create file in temp dir: " + e.getMessage());
                        throw new RuntimeException(e); // Propagate critical error
                    }

                    outputFileHash.put(appID, testOutFile);
                    // File testOutFile = new File("outputFiles" + appID);
                    log.debug("this is the output file that i make :getPath()  " + testOutFile.getPath()
                            + " getAbsolutePath()  " + testOutFile.getAbsolutePath());

                    outputFileHash.put(appID, testOutFile);
                    log.debug("Output file prepared at: " + outputFileHash.get(appID).getAbsolutePath());
                    workHash.put(appID, 0);

                    aws.send_msg(threadManagerQueueUrl, body);
                    System.out.println("message send from Manager");
                }

                try {
                    aws.deleteMsgFromQueue(appManagerQueueUrl, message.receiptHandle());
                    log.debug("Message was deleted");
                } catch (SqsException e) {
                    log.error("Error while deleting the message: "
                            + e.awsErrorDetails().errorMessage());
                }
            }
        }
    }

    private void recieveNewApp2() throws Exception {
        while (!isGetTerminateMsg) {
            while (aws.getQueueSize(appManagerQueueUrl) == 0) {
                sleep(20000);
            }

            List<Message> messages = aws.recive_msg(appManagerQueueUrl);

            for (Message message : messages) {
                String body = message.body();

                if (body.equals(SpecialMessages.TERMINATE.toString())) {
                    System.out.println("Manager Recieve Terminate Massage");
                    isGetTerminateMsg = true;
                } else {
                    String[] parts = parseMsg(body);
                    String inputPath = parts[0];
                    String appID = parts[1];
                    /*
                     * int n = Integer.parseInt(parts[2]);
                     * int totalLines = getNumOfLine(inputFileHash.get(appID));
                     * int increasedWorkers = (totalLines + n - 1) / n;
                     */
                    int increasedWorkers = Integer.parseInt(parts[2]);
                    synchronized (ActiveWorkerNumber) {
                        int val = 0;
                        for (int i = increasedWorkers; ActiveWorkerCount < aws.MAX_TOTAL_INSTANCES && i > 0; i--) {
                            ActiveWorkerCount++;
                            val++;
                        }
                        aws.start_worker(aws.ami, val, Work_Type.WORKER);
                        IdToAddedWorkers.put(appID, val);
                        /*
                         * int canLaunch = Math.min(increasedWorkers,aws.MAX_TOTAL_INSTANCES -
                         * ActiveWorkerCount);
                         * aws.start_worker(aws.ami, canLaunch, Work_Type.WORKER);
                         * IdToAddedWorkers.put(appID, canLaunch);
                         * ActiveWorkerCount += canLaunch;
                         */

                        /*
                         * List<Instance> launched = aws.start_worker(aws.ami, increasedWorkers,
                         * Work_Type.WORKER);
                         * int actuallyLaunched = (launched == null) ? 0 : launched.size();
                         * ActiveWorkerCount += actuallyLaunched;
                         * IdToAddedWorkers.put(appID, actuallyLaunched);
                         * System.out.println("Requested " + increasedWorkers +
                         * " workers, actually launched " + actuallyLaunched);
                         */
                    }
                    aws.downloadAndAddToHash(inputPath, appID, inputFileHash);
                    // File testOutFile = new File("outputFiles" + appID);
                    File testOutFile = new File(System.getProperty("java.io.tmpdir"), "outputFiles" + appID);
                    if (!testOutFile.exists()) {
                        try {
                            if (testOutFile.createNewFile()) {
                                log.debug("Successfully created new output file: " + testOutFile.getAbsolutePath());
                            } else {
                                log.error("Failed to create new output file: " + testOutFile.getAbsolutePath());
                                // Handle the error (maybe skip this application)
                            }
                        } catch (java.io.IOException e) {
                            log.error("IO Exception while creating output file: " + e.getMessage());
                            // Re-throw or handle the severe error
                        }
                    }
                    log.debug("this is the output file that i make :getPath()  " + testOutFile.getPath()
                            + " getAbsolutePath()  " + testOutFile.getAbsolutePath());
                    outputFileHash.put(appID, testOutFile);
                    workHash.put(appID, 0);
                    aws.send_msg(threadManagerQueueUrl, body);
                    System.out.println("message send from Manager");
                }
                try {
                    aws.deleteMsgFromQueue(appManagerQueueUrl, message.receiptHandle());
                    System.out.println("Message was deleted");
                } catch (SqsException e) {
                    System.err.println("Error while deleting the message: " + e.awsErrorDetails().errorMessage());
                }
            }
        }
    }

    private void recieveNewApp4() throws Exception {
        while (!isGetTerminateMsg) {
            while (aws.getQueueSize(appManagerQueueUrl) == 0) {
                sleep(20000);
            }

            List<Message> messages = aws.recive_msg(appManagerQueueUrl);

            for (Message message : messages) {
                String body = message.body();

                if (body.equals(SpecialMessages.TERMINATE.toString())) {
                    System.out.println("Manager Recieve Terminate Massage");
                    isGetTerminateMsg = true;
                } else {
                    String[] parts = parseMsg(body);
                    String inputPath = parts[0];
                    String appID = parts[1];
                    /*
                     * int n = Integer.parseInt(parts[2]);
                     * int totalLines = getNumOfLine(inputFileHash.get(appID));
                     * int increasedWorkers = (totalLines + n - 1) / n;
                     */
                    int increasedWorkers = Integer.parseInt(parts[2]);
                    synchronized (ActiveWorkerNumber) {
                        /*
                         * int val = 0;
                         * for(int i = increasedWorkers ; ActiveWorkerCount < maxWorkers && i > 0;i--){
                         * ActiveWorkerCount++;
                         * val++;
                         * }
                         * aws.start_worker(aws.ami, val, Work_Type.WORKER);
                         * IdToAddedWorkers.put(appID, val);
                         */
                        /*
                         * int canLaunch = Math.min(increasedWorkers,aws.MAX_TOTAL_INSTANCES -
                         * ActiveWorkerCount);
                         * aws.start_worker(aws.ami, canLaunch, Work_Type.WORKER);
                         * IdToAddedWorkers.put(appID, canLaunch);
                         * ActiveWorkerCount += canLaunch;
                         */

                        List<Instance> launched = aws.start_worker(aws.ami, increasedWorkers, Work_Type.WORKER);
                        int actuallyLaunched = (launched == null) ? 0 : launched.size();
                        ActiveWorkerCount += actuallyLaunched;
                        IdToAddedWorkers.put(appID, actuallyLaunched);
                        System.out.println(
                                "Requested " + increasedWorkers + " workers, actually launched " + actuallyLaunched);
                    }
                    aws.downloadAndAddToHash(inputPath, appID, inputFileHash);
                    outputFileHash.put(appID, new File("outputFiles" + appID));
                    workHash.put(appID, 0);
                    aws.send_msg(threadManagerQueueUrl, body);
                    System.out.println("message send from Manager");
                }
                try {
                    aws.deleteMsgFromQueue(appManagerQueueUrl, message.receiptHandle());
                    System.out.println("Message was deleted");
                } catch (SqsException e) {
                    System.err.println("Error while deleting the message: " + e.awsErrorDetails().errorMessage());
                }
            }
        }
    }

    private void sendMsgToWorker() throws Exception {
        while (true) {
            while (!isStopThreads && aws.getQueueSize(threadManagerQueueUrl) == 0) {
                sleep(10000);
            }
            if (isStopThreads) {
                break;
            }

            List<Message> managerMessages = aws.recive_msg_form_sqs(threadManagerQueueUrl);
            for (Message message : managerMessages) {
                String body = message.body();

                String inputPath = parseMsg(body)[0];
                String appId = parseMsg(body)[1];
                int n = Integer.parseInt(parseMsg(body)[2]);
                File file = inputFileHash.get(appId);
                System.out.println("----------------for debug----------------");
                System.out.println("App Id: " + appId);
                System.out.println("Input Path: " + inputPath);
                System.out.println("n: " + n);
                System.out.println("The number of lines in file is: " + file.getAbsolutePath() + getNumOfLine(file));
                System.out.println("------------------------------------------");
                String firstLine = "";
                int sendedMessegesCount = 0;
                for (int i = 0; i < messagePerApp && file.length() != 0; i++) {
                    firstLine = getFirstLineAndRemove(file);
                    String msgToWorker = "POS" + "$" + firstLine + "$" + "none" + "$" + appId + "$" + "none" + "$"
                            + "1";
                    String msgToWorker1 = firstLine + "$" + appId + "$" + "1";

                    aws.send_msg(threadWorkersQueueUrl, msgToWorker1);
                    System.out.println("managerToWorker thread send message to worker");
                    sendedMessegesCount++;
                }
                System.out.println("------------------------------------------");

                synchronized (workingHashObject) {
                    int v = workHash.get(appId);
                    workHash.remove(appId);
                    workHash.put(appId, v + sendedMessegesCount);
                }

                if (file.length() == 0) {
                    try {
                        aws.deleteMessage(message, threadManagerQueueUrl);
                        System.out.println("all apps messages sented to workers");
                    } catch (SqsException e) {
                        System.err.println("Error deleting msg: " + e.awsErrorDetails().errorMessage());
                    }
                    inputFileHash.remove(appId);
                }
            }
        }
    }

    /**
     * This function can get two different type of msg and split the message
     * accordingly:
     * 1.Application Message: split it to array that include [inputFileUrl ,
     * applicationId , n]
     * 2.Worker Message: split it to array that include
     * [operation(POS/CONSTITUENCY/DEPENDENCY) , inputFileUrl , outputFileUrl ,
     * applicationId , errorText(No need in this code) , flag(success = "1", error =
     * "0")]
     * 
     * @param msg : The message from one of this two types
     * @return array of the parts of this message
     */
    private String[] parseMsg(String msg) {
        return msg.split("\\$");
    }

    private static int getNumOfLine(File file) {
        int count = 0;
        try (BufferedReader b = new BufferedReader(new FileReader(file))) {
            while (b.readLine() != null) {
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    private static String getFirstLineAndRemove(File file) throws Exception {
        List<String> lines = Files.readAllLines(file.toPath());
        if (lines.isEmpty()) {
            throw new IIOException("File is Empty");
        }
        String first = lines.get(0);
        lines.remove(0);
        Files.write(file.toPath(), lines);
        return first;
    }

    private static void addLineToFile(File file, String line) throws Exception {
        try (BufferedWriter b = new BufferedWriter(new FileWriter(file, true))) {
            b.write(line);
            b.newLine();
        } catch (Exception e) {
            log.error("Error while writing to file: " + file.getName() + " (" + e.getMessage() + ")");
            // log.error("Error while writing to file: " + e.getMessage());
            // throw new Exception(e);
        }
    }

    private boolean isLastMessage(String id) {
        return (!inputFileHash.containsKey(id)) && workHash.get(id) == 0;
    }

    public void cleanup() {
        System.out.println("Manager starts cleaning up");
        try {
            sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (ActiveWorkerCount > 0) {
            aws.deleteInstanceByWorkType(ActiveWorkerCount, aws.ami, Work_Type.WORKER);
        }

        aws.deleteQueue(appManagerQueueUrl);
        aws.deleteQueue(threadManagerQueueUrl);
        aws.deleteQueue(threadWorkersQueueUrl);
        aws.deleteQueue(managerToWorkerQueueUrl);
        aws.deleteQueue(managerToAppQueueUrl);

        aws.deleteAllObjectsFromBucket(bucketName);
        aws.deleteEmptyBucket(bucketName);

        System.out.println("Cleanup completed");
        System.out.println("Manager shuting down");
        aws.deleteInstanceByWorkType(1, aws.ami, Work_Type.MANAGER);

    }
}
