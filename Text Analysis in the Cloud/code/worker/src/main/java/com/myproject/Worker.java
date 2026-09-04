package com.myproject;

import java.util.Properties;
import com.myproject.common.AWS;
import com.myproject.common.myLogger;
import com.myproject.common.s3_debuge;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.myproject.common.AnalysisType;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Worker {
    static StanfordCoreNLP pipeline;

    private static StanfordCoreNLP getPipeline(AnalysisType type) {
        Properties props = new Properties();

        switch (type) {
            case POS:
                props.setProperty("annotators", "tokenize,ssplit,pos");
                break;

            case CONSTITUENCY:
                props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse");
                break;

            case DEPENDENCY:
                props.setProperty("annotators", "tokenize,ssplit,pos,lemma,depparse");
                break;

            default:
                throw new IllegalArgumentException("Unknown analysis type: " + type);
        }

        return new StanfordCoreNLP(props);
    }

    // private static final Logger log = LoggerFactory.getLogger(Worker.class);
    // msg will be like --> taskId#bucketName#inputObjectKey#otherData... --->
    // separated by #
    static String from_worker_to_manager_queue = "manager_To_Worker_Queue_Name";// the msg to send to boss--> for result
    static String from_manager_to_worker_queue = "thread_Worker_Queue_Name";// get the msg from the boss-->for task

    static myLogger logger = new myLogger(Worker.class);
    static AWS aws = AWS.getInstance();
    static boolean terminate = false;

    static long time_of_sleeping = 30000; // sleep for 3000 == 0.5 min

    static String output_path = "output";// todo:check that
    static File output_file = new File(output_path);

    public static void main(String[] args) {
        logger.debug("NEW WORKER");
        new s3_debuge(aws.getS3Client(), aws.bucketName, "logs/app-log.txt").log("worker",
                "a new worker start in work ");

        Worker worker = new Worker();
        try {
            worker.processMessage();
        } catch (Exception e) {
        }
    }

    public void processMessage() {
        System.out.println("Hello World");
        logger.info(" a new worker start in work ");

        // all the worker life
        while (!terminate) {

            try {
                if (aws.getQueueSize(aws.getQueueUrl(from_manager_to_worker_queue)) == 0) {
                    logger.info("still not get a task ");
                    // sleep until get it
                    sleep();
                    continue;
                }

                // recive msg
                logger.info("get the task");
                Message task_message = get_jop();
                System.out.println("massege that worker id   is : " + task_message.body());
                if (no_task(task_message)) {
                    sleep();
                    continue;
                }

                String body = task_message.body();
                logger.info("Received message: " + body);
                // there msg not null

                // check if the msg is to end the work
                if (check_if_finish(task_message)) {
                    logger.info("finished task");
                    kill(task_message);
                    break;
                }

                String work_result;
                try {
                    // there a real task do it
                    work_result = work(body);
                } catch (Exception e) {
                    // send an error message for the manager
                    logger.error("Error while processing task: " + e.getMessage());
                    work_result = buildErrorResult(body, e);
                }

                // send the result to boss
                send_result(work_result);

                // delete it from the queue
                delete_msg_from_queue(task_message);

            } catch (Exception e) {
                logger.error(e.getMessage());
            }

        }
        logger.info(" worker finished bye");
        new s3_debuge(aws.getS3Client(), aws.bucketName, "logs/app-log.txt").log("Worker", "worker finished bye");

    }

    /**
     * Send a "done" or "error" message to the manager queue.
     * 
     * @param workResult the result if there //todo : check if need it
     */
    private static void send_result(String workResult) {
        aws.send_msg_queue_name(from_worker_to_manager_queue, workResult);
    }

    /**
     * delete the msg from the queue after finish the jop
     * 
     * @param taskMessage the msg to delete
     */
    private static void delete_msg_from_queue(Message taskMessage) {
        aws.delete_msg_from_queue(from_manager_to_worker_queue, taskMessage);// todo zaki implement that - make it get
                                                                             // the name and found the url
    }

    private static boolean no_task(Message taskMessage) {
        return (taskMessage == null || taskMessage.body() == null);
    }

    /**
     * delete the msg and terminate the worker
     * 
     * @param task_message
     */
    private static void kill(Message task_message) {
        logger.info("kill worker ");
        delete_msg_from_queue(task_message);// todo :check if there a need to terminate the ec2 from here or in manager
        terminate = true;
    }

    /**
     * Build an error result message when analysis fails.
     */
    private static String buildErrorResult(String originalBody, Exception e) {
        String[] parts = originalBody.split("\\$");
        String operationAndUrl = parts.length > 0 ? parts[0] : "UNKNOWN";
        String url = "";
        String analysisStr="";
        try{
        String[] opParts = parts[0].trim().split("\\s+", 2);
        if (opParts.length != 2) {
            throw new IllegalArgumentException("Invalid operation + URL format.");
        }

         analysisStr = opParts[0]; // POS / CONSTITUENCY / DEPENDENCY
         url = opParts[1]; // input file URL
        } catch (Exception ex) {
            try {
            String[] opParts = parts[0].trim().split("\t", 2);
            analysisStr = opParts[0]; // POS / CONSTITUENCY / DEPENDENCY
            url = opParts.length > 1 ? opParts[1].trim() : "ERROR"; // URL or empty if missing
            } catch (Exception loggingEx) {
            analysisStr = "ERROR";
            url = "ERROR";
    // Ignore logging errors
            }
            
        }
        String appId = parts.length > 1 ? parts[1] : "UNKNOWN";

        String errorText = e.getClass().getSimpleName() + ": " + e.getMessage();

        return analysisStr + "$" + url + "$" + "" + "$" + appId + "$" + errorText + "$0";
    }



    /*
     * private static String work(String body) throws Exception {
     * String[] msg_arr = seprate_msg(body);
     * if (msg_arr.length != 3) {
     * throw new
     * IllegalArgumentException("Task not in the expected format: analysisType#inputUrl#jobId"
     * );
     * }
     * 
     * // todo: check what eactly send and get it from the array
     * String analysisType = msg_arr[0]; // POS / CONSTITUENCY / DEPENDENCY
     * String inputUrl = msg_arr[1];
     * String jobId = msg_arr[2];
     * 
     * // ▪ Downloads the text file indicated in the message.
     * File input_file_downloaded = downloadFromHttp(inputUrl);// todo:check if this
     * the right function
     * // Performs the requested analysis on the file
     * output_file = analysis(input_file_downloaded,
     * AnalysisType.valueOf(analysisType));
     * 
     * // Puts a message in an SQS queue indicating the original URL of the input
     * file,
     * // the S3 url of the
     * // analyzed file, and the type of the performed analysis
     * // String s3Key = "analysis/" + jobId + "-" + System.currentTimeMillis() +
     * // ".txt";
     * String s3Url = aws.uploadFile(output_file.getAbsolutePath(), aws.bucketName);
     * String result = analysisType + ":<" + inputUrl + "><" + s3Url + ">" +
     * jobId;//<analysis type>: <input file> <output file> <analysis type>: <input
     * file> <a short description of the exception>
     * return result;
     * }
     */
    private static String work(String body) throws Exception {

        logger.info("Starting work on task: " + body);
        // Expected format: "POS https://url...$appId$1"
        String[] parts = body.split("\\$");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid message format: analysisType URL$appId$flag");
        }

        // Extract "POS https://..." into ["POS","https://..."]
        String[] opParts = parts[0].trim().split("\\s+", 2);
        if (opParts.length != 2) {
            throw new IllegalArgumentException("Invalid operation + URL format.");
        }

        String analysisStr = opParts[0]; // POS / CONSTITUENCY / DEPENDENCY
        String url = opParts[1]; // input file URL
        String appId = parts[1]; // app ID
        String flag = parts[2]; // success flag from Manager (we ignore)

        // Convert analysis type string → enum
        AnalysisType analysisType = AnalysisType.valueOf(analysisStr);

        // Download the file (REAL NLP)
        File downloaded = downloadFromHttp(url);

        // Perform analysis
        pipeline = getPipeline(analysisType);

        File resultFile = analysis(downloaded, analysisType);

        check(resultFile);

        // Upload result to S3
        String s3Url = aws.uploadFile(resultFile.getAbsolutePath(), aws.bucketName);
        s3Url = "https://" + aws.bucketName + ".s3.amazonaws.com/" + resultFile.getName();
        s3Url=aws.generatePresignedUrl(aws.bucketName, resultFile.getName());
        logger.info("Open this URL in browser: " + s3Url);

        // Build response for Manager.parseMsg()
        return analysisType.name() + "$" +
                url + "$" +
                s3Url + "$" +
                appId + "$" +
                "" + "$" +
                "1"; // SUCCESS FLAG
    }

    private static void check(File resultFile) {
        if (resultFile == null || !resultFile.exists()) {
            throw new IllegalStateException("Analysis result file is missing.");
        }
        if (resultFile.length() == 0) {
            throw new IllegalStateException("Analysis result file is empty.");
        }
        String p = resultFile.getName().toLowerCase();
        if (!p.endsWith(".txt")) {
            throw new IllegalStateException("Analysis result file must end with .txt but got: " + resultFile);
        }

    }

    public static File analysis(File input, AnalysisType analysisType) throws IOException {
        switch (analysisType) {
            case POS: {
                return posAnalysis(input);
            }
            case CONSTITUENCY: {
                return constituencyAnalysis(input);
            }
            case DEPENDENCY: {
                return dependencyAnalysis(input);

            }
            default: {
                logger.error("Unknown analysis type: " + analysisType);
                throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
            }
        }
    }

    private static File dependencyAnalysis(File input) throws IOException {
        String text = Files.readString(input.toPath());

        File out = File.createTempFile("dependency-output-", ".txt");
        try (var fw = new java.io.FileWriter(out)) {

            String[] sentences = text.split("[.!?]+\\s*");

            for (String sentence : sentences) {
                sentence = sentence.trim();

                if (sentence.length() < 5 || sentence.length() > 400)
                    continue;

                Annotation ann = new Annotation(sentence);

                try {
                    pipeline.annotate(ann);
                } catch (Exception e) {
                    System.out.println("Skipped sentence: " + sentence);
                    continue;
                }

                for (CoreMap s : ann.get(CoreAnnotations.SentencesAnnotation.class)) {

                    SemanticGraph deps = s.get(
                            SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);

                    for (SemanticGraphEdge edge : deps.edgeListSorted()) {
                        fw.write(edge.getRelation() + "("
                                + edge.getGovernor().word() + ", "
                                + edge.getDependent().word() + ")");
                        fw.write(System.lineSeparator());
                    }
                    fw.write(System.lineSeparator());
                }
            }
        }

        return out;
    }

    private static File posAnalysis(File input) throws IOException {
        String text = Files.readString(input.toPath());

        File out = File.createTempFile("pos-output-", ".txt");
        try (var fw = new java.io.FileWriter(out)) {

            String[] sentences = text.split("[.!?]+\\s*");

            for (String sentence : sentences) {
                sentence = sentence.trim();
                if (sentence.length() < 5 || sentence.length() > 400)
                    continue;

                Annotation ann = new Annotation(sentence);

                try {
                    pipeline.annotate(ann);
                } catch (Exception e) {
                    continue;
                }

                for (CoreMap s : ann.get(CoreAnnotations.SentencesAnnotation.class)) {
                    for (CoreLabel token : s.get(CoreAnnotations.TokensAnnotation.class)) {
                        fw.write(token.word() + "/" +
                                token.get(CoreAnnotations.PartOfSpeechAnnotation.class) + " ");
                    }
                    fw.write(System.lineSeparator());
                }
            }
        }

        return out;
    }

    private static File constituencyAnalysis(File input) throws IOException {
        String text = Files.readString(input.toPath());

        File out = File.createTempFile("constituency-output-", ".txt");
        try (var fw = new java.io.FileWriter(out)) {

            String[] sentences = text.split("[.!?]+\\s*");

            for (String sentence : sentences) {
                sentence = sentence.trim();
                if (sentence.length() < 5 || sentence.length() > 400)
                    continue;

                Annotation ann = new Annotation(sentence);

                try {
                    pipeline.annotate(ann);
                } catch (Exception e) {
                    continue;
                }

                for (CoreMap s : ann.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree t = s.get(TreeCoreAnnotations.TreeAnnotation.class);
                    fw.write(t.toString());
                    fw.write(System.lineSeparator());
                }
            }
        }

        return out;
    }

    /**
     * recive the task from the boss to do
     * 
     * @return the task that recive or null if there no tasks
     */
    private static Message get_jop() {
        List<Message> msg = aws.recive_msg_form_sqs_queue_name(from_manager_to_worker_queue);
        return (msg == null || msg.isEmpty()) ? null : msg.get(0);
    }

    /**
     * check if the msg that get is to terminate
     * 
     * @param task_message the msg that get from boss
     * @return true if get the command terminate
     */
    private static boolean check_if_finish(Message task_message) {
        return "TERMINATE".equals(task_message.body());// equal(Terminate.1)//todo: zaki change it to use the enum
    }

    /**
     * get the thread sleep and wait
     */
    private static void sleep() {
        try {
            Thread.sleep(time_of_sleeping);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static File downloadFromHttp(String urlString) throws IOException {
        java.net.URL url = new java.net.URL(urlString);
        File temp = File.createTempFile("worker-input-", ".txt");
        try (var in = url.openStream()) {
            Files.copy(in, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return temp;
    }

}