# Distributed System Programming — AWS Assignments

This repository contains three assignments completed for the **Distributed System Programming** course. The assignments demonstrate progressively more advanced distributed-computing concepts using Amazon Web Services and Java.

## Group Members

- Mahmoud Haj Yahya
- Zaki Masarwa

## Repository Structure

```text
.
├── hw1/
│   ├── assignment/
│   └── code/
├── hw2/
│   ├── assignment/
│   └── code/
├── hw3/
│   ├── assignment/
│   └── code/
└── README.md
```

Each homework directory contains the assignment instructions and the corresponding implementation. Detailed build and execution instructions are available in the README file inside each `code` directory.

## AWS Services Used

The assignments use several AWS services for storage, communication, computation, and large-scale data processing:

| AWS Service | Purpose in the Assignments |
|---|---|
| **Amazon EC2** | Runs Manager and Worker instances or provides the instances used by EMR clusters. |
| **Amazon S3** | Stores input files, JAR files, intermediate results, final outputs, and logs. |
| **Amazon SQS** | Enables asynchronous communication between the Local Application, Manager, and Workers in HW1. |
| **Amazon EMR** | Runs Hadoop MapReduce pipelines for large-scale text processing in HW2 and HW3. |

---

## HW1 — Distributed Text Analysis in the Cloud

HW1 implements a distributed text-analysis system using a **Local Application, Manager, and multiple Workers**.

The Local Application uploads an input file to S3 and sends a processing request through SQS. The Manager creates and supervises EC2 Worker instances, distributes the tasks, collects the results, and returns a summary to the Local Application. Each Worker uses Stanford CoreNLP to analyze its assigned text.

### Main AWS Usage

- **EC2:** Runs the Manager and Worker instances.
- **SQS:** Transfers tasks and status messages between the system components.
- **S3:** Stores input files, JAR files, Worker results, and the final summary.

### Purpose

The purpose of this assignment is to practice:

- Designing a distributed Manager–Worker architecture.
- Dividing work between multiple cloud instances.
- Implementing asynchronous message-based communication.
- Handling Worker failures and message visibility timeouts.
- Managing resources, scalability, termination, and cloud storage.

For full build and execution instructions, see [`hw1/code/README.md`](hw1/code/README.md).

---

## HW2 — Collocation Extraction with MapReduce

HW2 implements a multi-step Hadoop MapReduce system that extracts the **100 strongest word collocations for each decade** from the Google Books N-Gram datasets in English or Hebrew.

The system processes unigram and bigram counts, calculates the Log-Likelihood Ratio (LLR) for candidate word pairs, and sorts the strongest collocations. It also supports execution with or without a Combiner so the effect of local aggregation can be examined.

### Main AWS Usage

- **EMR:** Executes the four MapReduce steps on a Hadoop cluster.
- **S3:** Provides the Google Books input datasets and stores intermediate and final outputs.
- **EC2:** Supplies the computing instances managed by the EMR cluster.

### Purpose

The purpose of this assignment is to practice:

- Implementing a multi-step MapReduce pipeline.
- Processing very large linguistic datasets.
- Designing Mapper, Reducer, and Combiner logic.
- Joining information produced by different MapReduce jobs.
- Comparing executions with and without local aggregation.
- Ranking statistical results by decade.

For full build and execution instructions, see [`hw2/code/README.md`](hw2/code/README.md).

---

## HW3 — Lexico-Syntactic Similarity Extraction

HW3 implements the **DIRT algorithm** for discovering similarities between lexico-syntactic dependency paths. It processes the Google Syntactic N-Grams Biarcs dataset through a seven-step MapReduce pipeline.

The system extracts valid dependency paths, calculates path and slot statistics, computes mutual-information feature values, builds feature vectors, joins them with positive or negative predicate-pair test sets, and produces a final similarity score for every tested pair.

### Main AWS Usage

- **EMR:** Executes the seven Hadoop MapReduce jobs.
- **S3:** Stores the application JAR, Biarcs input files, test sets, intermediate outputs, final similarity scores, and logs.
- **EC2:** Provides the Master and Core instances used by the EMR cluster.

### Purpose

The purpose of this assignment is to practice:

- Building a complex multi-stage distributed data-processing pipeline.
- Processing syntactic dependency information at scale.
- Designing intermediate keys and values for chained MapReduce jobs.
- Calculating mutual information and vector-based similarity.
- Implementing and evaluating a research algorithm using cloud resources.
- Managing large EMR jobs, outputs, logs, and AWS costs.

For full build and execution instructions, see [`hw3/code/README.md`](hw3/code/README.md).

---

## Overall Learning Progression

The three assignments progress from a custom distributed architecture to increasingly complex Hadoop workflows:

1. **HW1:** Build and coordinate a distributed cloud application manually using EC2, SQS, and S3.
2. **HW2:** Use EMR and MapReduce to analyze large-scale N-Gram datasets through several processing stages.
3. **HW3:** Apply an advanced seven-stage MapReduce pipeline to implement a research-based semantic similarity algorithm.

Together, the assignments demonstrate distributed communication, parallel execution, fault handling, scalable storage, MapReduce design, and large-scale natural-language processing on AWS.
