package com.myproject;

import com.myproject.common.AWS;
import com.myproject.common.AnalysisType;
import com.myproject.common.myLogger;

import edu.stanford.nlp.io.EncodingPrintWriter.out;

import com.myproject.common.SpecialMessages;

import software.amazon.awssdk.services.sqs.model.Message;

//import static com.myproject.LocalApplication.myLogger;

import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LocalApplication {

    static Long start_timeLong;

    static boolean stop_running = false;
    static String applicationId = UUID.randomUUID().toString();

    public static final myLogger p = new myLogger(LocalApplication.class);
    static AWS aws = AWS.getInstance();
    static File input_file;
    static File html_output_file;
    static File outputFile;
    // where the input and output file saved
    static String relativePath = "src/main/resources/";
    private static String inputFileName;
    private static String outputFileName;
    private static int n;// max file per worker
    private static boolean terminate;
    private static String backet_name = AWS.getInstance().bucketName;// ;"aws_ass_backet_name_2026";
    private static String from_app_to_manager_queue = "app-manager_queue";

    // static Path inPath;
    // static Path outPath;
    private static String from_manager_to_app_queue = "manager-app_queue";
    private static long sleepTimeMillis = 30000;

    // jars
    private static final String JAR_DIR = "/AWSCloud/target/";

    private static final String worker_jar = "/AWSCloud/worker/target/worker-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String manager_jar = "/AWSCloud/manager/target/manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String worker_jar_S3 = "worker-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String manager_jar_s3 = "manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
    // private static String manager_jar =
    // "manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
    // private static String worker_jar =
    // "worker-1.0-SNAPSHOT-jar-with-dependencies.jar";
    // private static void createEC2() {
    // String ec2Script = "#!/bin/bash\n" +
    // "echo Hello World\n";
    // String managerInstanceID = aws.createEC2(ec2Script, "thisIsJustAString", 1);
    // }

    private static void setUp(String[] args) {
        // get the parmetres
        inputFileName = args[0];
        outputFileName = args[1];
        n = Integer.parseInt(args[2]);

        // DO THE WORK AND TERMINATE ALL
        terminate = (args.length == 4 && args[3].equals("terminate"));

        String relativePath = "/AWSCloud/localapp/src/main/resources/";

        // in main, after you finish one job

        // prepare all files
        input_file = new File(relativePath + inputFileName);// awscloud/src/ .. .. .
        html_output_file = new File(relativePath + outputFileName);
        outputFile = new File("/AWSCloud/localapp/src/main/resources/output_not_html.txt");

        aws.createBucketIfNotExists(backet_name);
        try {
            if (!aws.s3ObjectExists(aws.bucketName, manager_jar_s3)) {
                p.info("upload the manager jar to s3 will take time ... ");
                aws.uploadFile(
                        manager_jar,
                        aws.bucketName);
            }
            if (!aws.s3ObjectExists(aws.bucketName, worker_jar_S3)) {
                p.info("upload the worker jar to s3 will take time ... ");
                aws.uploadFile(
                        worker_jar,
                        aws.bucketName);
            }
        } catch (IOException e) {
            p.error("ERROR uploading JAR files to S3: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        // Checks if a Manager node is active on the EC2 cloud. If it is not, the
        // application will start the
        // manager node.

        boolean check_manager = aws.is_boss_work();

        if (!check_manager) {
            System.out.println("No active Manager found, creating one...");
            aws.start_manager();
        }

    }

    public static void main(String[] args) {
        // start time
        start_timeLong = System.currentTimeMillis();

        if (args == null || args.length == 0)
            p.debug("no args ");
        else {
            for (String a : args) {
                p.debug("the arg is: " + a);
            }

            if (args != null && args.length > 0 && args[0].equals("terminate")) {
                send_termination_message_to_sqs();

                p.info("Termination message sent to Manager.");
                stop_running = true;
            }
        }
        setUp(args);

        while (!stop_running) {

            // String backet_to_add = backet_name;//+ aws.backet_count.getAndIncrement();

            String inPath = input_file.getPath(); // get the path of the input file
            // Uploads the input file to S3.
            System.out.println("Uploads the input file to S3." + inPath.toString() + " backet id " + backet_name);
            String inputFileUrl = upload_file_to_s3(input_file.getPath(), backet_name);

            // Sends a message to an SQS queue, stating the location of the file on S3
            // System.out.println("sending msg to sqs with the url " + aws.getQueueUrl());
            if (!(aws.Queue_exist(from_app_to_manager_queue))) {
                System.out.println("there is no qoue then creating ....");
                aws.createSqsQueue(from_app_to_manager_queue);

            }
            String appId = applicationId;// outputFileName;
            // String message_to_send = inputFileName + "$" + backet_to_add + "$" + n;//
            String message_to_send = inputFileUrl + "$" + appId + "$" + n;
            // backet name
            send_message_to_sqs(aws.getQueueUrl(from_app_to_manager_queue), message_to_send);
            p.debug("SEND : " + message_to_send);
            // wait yahbib until the boss answer
            boolean answered = false;

            while (!answered) {
                sleep();
                // before check if the boss do her work and create the queue
                if (!aws.Queue_exist(from_manager_to_app_queue)) {
                    System.out.println(
                            "Manager->App queue '" + from_manager_to_app_queue + "' not ready yet, waiting...");
                    continue; // skip this iteration, go back to sleep
                }
                // the manager -> app queue exist now we can continue, thank boss
                answered = (aws.getQueueSize(aws.getQueueUrl(from_manager_to_app_queue)) != 0);// if the size of the
                // queue is still 0 then
                // not answerd yet
            }

            p.info("recive the manager message");

            List<Message> managerMessage = receive_message_from_sqs();
            if (managerMessage == null || managerMessage.isEmpty()) {
                p.info(" no message in the queue ");
                answered = false;
                continue;
            }
            // itrate ..
            for (Message msg : managerMessage) {
                String message_String = msg.body();
                p.debug("the message: " + message_String);
                download_file_from_s3(message_String, outputFile);
                p.info("file downloaded");
                convert_to_html(outputFile, html_output_file);
                p.info("file converted to html");
                aws.deleteMsgFromQueue(from_manager_to_app_queue, msg.receiptHandle());
                p.info(" message  deleted");
            }

            // Gets the summary output file from S3.

            // In case of terminate mode (as defined by the command-line argument), sends a
            // termination
            // message to the Manager.

            // we get the answer now can local stop
            stop_running = true;

            if (terminate) {
                send_termination_message_to_sqs();
                p.info("Termination message sent to Manager.");
                stop_running = true;
            }

        }
        cleanUp();

    }

    private static void cleanUp() {
        // aws.terminate();
        // it should get the termiante message
        // now time
        Long end_timeLong = System.currentTimeMillis();

        Long duration = end_timeLong - start_timeLong;
        duration = TimeUnit.MILLISECONDS.toMinutes(duration);
        p.info("Duration: " + duration + " minutes");
        p.info("that's it good bye ");
    }

    private static void convert_to_html(File input, File output) {
        try (
                BufferedReader reader = new BufferedReader(new FileReader(input));
                BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {

            writer.write("""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="UTF-8">
                        <title>Analysis Output</title>

                        <style>
                            body { font-family: Arial, sans-serif; background: #f5f7fa; padding: 20px; color: #333; }
                            h1 { text-align: center; color: #2c3e50; margin-bottom: 25px; }
                            table { width: 100%; border-collapse: collapse; background: white; border-radius: 10px;
                                    overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                            th { background: #34495e; color: #fff; padding: 12px; text-align: left; font-size: 16px; }
                            td { padding: 10px; border-bottom: 1px solid #ddd; font-size: 15px; }
                            tr:hover { background: #f1f1f1; }
                            .success { color: #27ae60; font-weight: bold; }
                            .error { color: #c0392b; font-weight: bold; }
                            a { color: #2980b9; text-decoration: none; }
                            a:hover { text-decoration: underline; }
                        </style>
                    </head>

                    <body>
                        <h1>Analysis Results</h1>
                        <table>
                            <tr>
                                <th>Analysis Type</th>
                                <th>Input File</th>
                                <th>Output / Error</th>
                            </tr>
                    """);

            String line;

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty())
                    continue;

                // Split by ANY amount of whitespace (spaces OR tabs)
                String[] parts = line.trim().split("\\s+", 3);

                String analysis, inputUrl, outputOrError;
                boolean isError = false;

                if (parts.length == 3) {
                    // Normal case
                    analysis = parts[0];
                    inputUrl = parts[1];
                    outputOrError = parts[2];
                    if(outputOrError.startsWith("ERROR") ||outputOrError.equals("error in file- is not available>") )
                        isError = true;
                    
                    if (analysis.equalsIgnoreCase("ERROR"))
                        isError = true;

                } else if (line.startsWith("ERROR") || line.startsWith("Error")) {
                    // Error cases like:
                    // ERROR ERROR ERROR error in file- is not available>
                    isError = true;

                    // Try best to extract meaningful fields
                    analysis = parts[0];// "ERROR";
                    if (analysis == null || analysis.isEmpty()) {
                        
                            analysis = "ERROR";// to avoid showing empty analysis type or not proprate  type 
                        
                    }
                    inputUrl = parts[1];// (parts.length > 1 ? parts[1] : "-");

                    outputOrError = line.substring(line.indexOf(inputUrl) + inputUrl.length()).trim();
                    if (outputOrError.isEmpty())
                        outputOrError = "Unknown error";

                } else {
                    // Skip unrecognized line
                    continue;
                }

                // Make clickable links
                String inputLink = inputUrl.startsWith("http") || inputUrl.startsWith("s3://")
                        ? "<a href=\"" + inputUrl + "\">" + inputUrl + "</a>"
                        : inputUrl;

                String outputLink = outputOrError.startsWith("http") || outputOrError.startsWith("s3://")
                        ? "<a href=\"" + outputOrError + "\">" + outputOrError + "</a>"
                        : outputOrError;

                writer.write("<tr>");
                writer.write("<td class=\"" + (isError ? "error" : "success") + "\">" + analysis + "</td>");
                writer.write("<td>" + inputLink + "</td>");
                writer.write("<td>" + outputLink + "</td>");
                writer.write("</tr>\n");
            }

            writer.write("""
                        </table>
                    </body>
                    </html>
                    """);

        } catch (Exception e) {
            p.error("HTML conversion error: " + e.getMessage());
        }
    }

    private static void convert_to_html12(File input, File output) {
        try (BufferedReader reader = new BufferedReader(new FileReader(input));
                BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {

            writer.write("<!DOCTYPE html>\n<html>\n<head>\n<title>Output File</title>\n</head>\n<body>\n<pre>\n");

            String line;
            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty())
                    continue;

                // Expected structure: TYPE INPUT OUTPUT
                String[] parts = line.split("\t", 3);

                if (parts.length == 3) {
                    // NORMAL SUCCESS LINE
                    String analysisType = parts[0];
                    String inputFile = parts[1];
                    String outputFile = parts[2];

                    writer.write(String.format("%s: %s %s", analysisType, inputFile, outputFile));
                } else if (parts.length == 2) {
                    // ERROR FORMAT: TYPE INPUT <error message missing>
                    writer.write(String.format("%s: %s %s", parts[0], parts[1], "<missing output>"));
                } else if (line.startsWith("Error:")) {
                    // Error line produced by Worker
                    // Format: "Error:TYPE INPUT MESSAGE"
                    String cleaned = line.substring(6).trim();
                    String[] errorParts = cleaned.split("\\s+", 3);

                    if (errorParts.length == 3) {
                        writer.write(String.format("%s: %s %s",
                                errorParts[0], errorParts[1], errorParts[2]));
                    } else {
                        writer.write(line);
                    }
                } else {
                    // Write unrecognized lines as-is
                    writer.write(line);
                }

                writer.newLine();
            }

            writer.write("</pre>\n</body>\n</html>");

        } catch (Exception exception) {
            p.error("HTML conversion error: " + exception.getMessage());
        }
    }

    /**
     * convert the output file to html file
     * 
     * @param input  input file
     * @param output output file
     */
    private static void convert_to_html1(File input, File output) {
        try {

            try (
                    BufferedReader reader = new BufferedReader(new FileReader(input));
                    BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
                writer.write(
                        "<!DOCTYPE html>\n<html>\n<head>\n<title>html output file</title>\n</head>\n<body>\n<pre>\n");

                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        String[] parts = line.split("\\s+", 3);

                        if (parts.length == 3) {
                            // <analysis type>: <input file> <output file>

                            if (parts[0].startsWith("Error:")) {
                                // <ERROR ANALYSIS type>: <input file> <error message>
                                // -> <analysis type>:<input file><error message>
                                parts[0] = parts[0].substring(6); // remove "Error:" prefix
                                String formattedLine = String.format("<%s><%s><%s>", parts[0], parts[1], parts[2]);
                                writer.write(formattedLine);
                            } else {
                                String formattedLine = String.format("<%s><%s><%s>", parts[0], parts[1], parts[2]);
                                writer.write(formattedLine);

                            }
                        } else if (parts.length == 2) {
                            String formattedLine = String.format("<%s>: <%s> <%s>", parts[0], parts[1], "");
                            writer.write(formattedLine);
                        } else {
                            // Write the original line if it doesn't match the expected structure
                            writer.write(line);
                        }
                        writer.newLine(); // Preserve line breaks

                    }
                    // writer.write(line);
                }

                writer.write("</pre>\n</body>\n</html>");
            }
        } catch (Exception exception) {
            p.error(exception.toString());
        }

    }

    private static void send_termination_message_to_sqs() {
        aws.send_msg(aws.getQueueUrl(from_app_to_manager_queue), SpecialMessages.TERMINATE.toString());

        System.out.println(" a  TERMINATE  message was sent to the manager");
    }

    private static void download_file_from_s3(String fileUrl, File outputFile) {
        try {
            p.debug("[download file from s3 ] this is my parmeter file url:    " + fileUrl + " output file "
                    + outputFile);
            aws.download_file_from_S3(fileUrl, outputFile);
        } catch (Exception e) {

            System.out.println("[ERROR][downloadFile] " + e.getMessage());
        }
    }

    private static List<Message> receive_message_from_sqs() {
        p.debug("receive the message from sqs queue");
        String queueUrl = aws.getQueueUrl(from_manager_to_app_queue);
        return aws.recive_msg_form_sqs(queueUrl);
    }

    private static void send_message_to_sqs(String queue_url, String message_to_send) {
        aws.send_msg(queue_url, message_to_send);
    }

    private static String upload_file_to_s3(String path, String backetNM) {

        p.debug(backetNM);
        p.debug(path);
        String url = "";
        try {
            File file = new File(path);
            if (!file.exists()) {
                throw new FileNotFoundException("heltawiiiii" + path);
            }
            url = aws.uploadFile(path, backetNM);
        } catch (Exception exception) {
            System.out.println("[ERROR UPLOAD FILE FUNC]" + exception.toString());
            System.exit(1);
        }
        // file path or file
        // if file path => dont create new file
        return url;
    }

    /**
     * bust wait for 0.5 min
     */
    private static void sleep() {
        try {

            System.out.println("Sleeping for 0.5 minutes...");
            Thread.sleep(sleepTimeMillis);

            System.out.println("Woke up!");
        } catch (InterruptedException e) {
            // Handle the interruption
            Thread.currentThread().interrupt();
        }
    }
}
