# Assignment 2 — Collocation Extraction with MapReduce

## Project Members

1. **Mahmoud Haj Yahya**
2. **Zaki Masarwa**

## How to Run the Project

### 1. Build the JAR File

From the project directory, run:

```bash
mvn clean package
```

### 2. Run the Project

```bash
java -cp target/CollocationExtraction_Project-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.dsp.collocation.util.AppStepRunner <WITH|WITHOUT> <ENG|HEB>
```

### Arguments

#### Combiner Mode

| Value | Meaning |
|---|---|
| `WITH` | Run with local aggregation using a combiner |
| `WITHOUT` | Run without local aggregation |

#### Language

| Value | Meaning |
|---|---|
| `ENG` | Run on the English dataset |
| `HEB` | Run on the Hebrew dataset |

### Examples

Run the English dataset with a combiner:

```bash
java -cp target/CollocationExtraction_Project-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.dsp.collocation.util.AppStepRunner WITH ENG
```

Run the Hebrew dataset without a combiner:

```bash
java -cp target/CollocationExtraction_Project-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.dsp.collocation.util.AppStepRunner WITHOUT HEB
```

## Notes

### 1. Input Datasets

#### English

- **Bigrams:**

```text
s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data
```

- **Unigrams:**

```text
s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data
```

#### Hebrew

- **Bigrams:**

```text
s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data
```

- **Unigrams:**

```text
s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data
```

### 2. Submitted Output

The `output` folder contains the final English and Hebrew results produced by Step 4 with a combiner. Runs with and without a combiner produce the same final output.

## MapReduce Steps

### Step 1 — Bigram Processing

- **Input:** Google Books 2-gram sequence files.
- **Output:**

```text
(decade, w1, w2) -> c12
```

### Step 2 — Unigram Aggregation

- **Input:** Google Books 1-gram sequence files.
- **Output:**

```text
(decade, w) -> c(word)
(decade, __TOTAL__) -> N(decade)
```

### Step 3 — Join and LLR Calculation

- **Input:**
  - Output of Step 1 containing the bigram counts.
  - Output of Step 2 containing the unigram counts and total counts.
- **Output:**

```text
(decade, w1, w2) -> (llr, c12, c1, c2)
```

### Step 4 — Top 100 Collocations per Decade

- **Input:** Output of Step 3.
- **Output:**

```text
(decade) -> (w1, w2, llr)
```
