# Assignment 1 — Text Analysis in the Cloud

## Group Members

1. **Zaki Masarwa**
2. **Mahmoud Haj Yahya**

## AWS Configuration

| Setting | Value |
|---|---|
| Region | `us-east-1` — N. Virginia |
| AMI | `ami-0fa3fe0fa7920f68e` |
| Maximum number of Workers | 18 |
| EC2 instance type | `t3.medium` |

## How to Run the Project

1. Build the JAR files:

```bash
mvn clean package
```

2. Move to the Local Application target directory:

```bash
cd /AWSCloud/localapp/target
```

3. Run the Local Application:

```bash
java -jar localapp-1.0-SNAPSHOT-jar-with-dependencies.jar input-sample.txt output.html n [terminate]
```

The `terminate` argument is optional.

### Arguments

| Argument | Description |
|---|---|
| `input-sample.txt` | Path to the local input file |
| `output.html` | Path or name of the HTML output file that is saved at the end of the run |
| `n` | Number of tasks per Worker |
| `terminate` | Optional argument that sends a termination message to the Manager after the job is completed |

## How the Program Works

### 1. Local Application

The Local Application performs the following operations:

- Runs the setup process and prepares the required files, including uploading the JAR files.
- Uploads the input file to S3.
- Checks whether a Manager instance is already running; otherwise, it starts a `t3.medium` EC2 instance.
- Sends a message to the SQS queue containing the location of the input file in S3.
- Waits for the Manager's response.
- Receives the Manager's message.
- Iterates over the received messages.
- Downloads the result file from S3.
- Converts the result to HTML.
- Deletes the processed message from the queue.
- Gets the summary output file from S3.
- When termination mode is enabled through the command-line argument, sends a termination message.

### 2. Manager

When the Manager starts, it initializes:

1. The Manager-to-Worker queue.
2. The Worker-to-Manager queue.

The Manager then processes messages received from the Application-to-Manager queue. It downloads the file from the URL included in the message, starts Workers according to the application's request, and sends the content of the input file together with the application ID to the Manager-to-Worker queue.

### 3. Worker

Each Worker performs the following operations:

- Downloads and opens the Worker JAR file.
- Polls messages from the Manager-to-Worker queue.
- Downloads and reads the text file line by line using `BufferedReader`; reading the entire file at once could cause an out-of-memory error.
- Analyzes each line using Stanford CoreNLP.
- Writes the result to a local template file.
- Uploads the result file to S3.
- Sends the result URL to the Manager in a message.

Finally, the Manager collects the URLs from the Worker-to-Manager queue, creates and uploads the summary file to S3, and notifies the Local Application of its location. The Local Application then downloads it. The Manager ensures optimal Worker usage and handles failures by starting replacement Workers when needed.

## Execution Information and Design Questions

### How long did the program take to finish processing the input file?

The program took approximately **11 minutes**.

### Did you think for more than two minutes about security?

Yes. The AWS credentials are hidden in the standard AWS credentials file:

```text
~/.aws/credentials
```

They are accessed from the file when the Manager needs them and are not written directly in the project source code.

### What value of `n` did you use, and why?

We used `n = 10` to balance system performance and workload.

### Did you consider scalability? Will the program work when one million, two million, or one billion clients connect simultaneously?

The program runs separately for each Local Application. Its distributed design allows multiple Local Applications to submit work, although the available AWS resources and configured instance limits determine the practical scale.

### What about persistence and failures?

If a Worker dies or stalls, the Manager starts a new Worker to complete the task. SQS visibility timeouts are used to ensure that messages are not lost and can be processed again when necessary. This also helps handle temporary communication failures.

### When are threads useful, and when are they harmful?

- **Good use:** When tasks need to be divided and executed concurrently by multiple threads.
- **Bad use:** Using too many threads creates unnecessary overhead.

### Did you run more than one client at the same time?

Yes.

### Do you understand how the system works?

Yes.

### Did you manage the termination process?

Yes.

### Did you consider the limitations of the system being used?

Yes.

### Are all Workers working hard, or are some of them idle? Why?

All Workers actively receive and complete tasks. They start, process their assigned work, and complete tasks concurrently.

### Is the Manager doing more work than it should? Are the responsibilities properly separated?

No. The Manager is responsible only for managing communication, assigning tasks, monitoring Workers, and collecting results. The analysis itself is performed by the Workers.

### What does distributed mean in this project? Is any part unnecessarily waiting for another part?

There are no unnecessary waits in the system. The work is distributed because tasks are divided among multiple EC2 instances and executed in parallel.
