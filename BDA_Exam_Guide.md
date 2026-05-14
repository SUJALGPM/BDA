# BDA Practical Exam Guide — Complete Reference

---

## Prerequisites (Run Before Everything Else)

```bash
# Verify Java 8
java -version
echo $JAVA_HOME

# If JAVA_HOME is wrong or empty
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# Set: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Check Hadoop
hadoop version

# Start Hadoop services
start-dfs.sh
start-yarn.sh
jps
# Should show: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode

# If services don't start
stop-all.sh
start-all.sh

# Still broken? Format NameNode (WARNING: deletes HDFS data)
hdfs namenode -format
start-all.sh

# Leave safe mode if stuck
hdfs dfsadmin -safemode leave

# If output directory already exists (re-run fix)
hdfs dfs -rm -r /output
```

---

## Universal Compilation Template

> Every program follows this same 5-step compile-and-run pattern. Only the filename, JAR name, class name, and arguments change.

```bash
# Step 1 — Create HDFS directory and upload input
hdfs dfs -mkdir /myfolder
hdfs dfs -put myinput.txt /myfolder

# Step 2 — Compile
rm -rf classes
mkdir classes
javac -classpath $(hadoop classpath) -d classes MyProgram.java

# Step 3 — Package into JAR
jar -cvf myprogram.jar -C classes/ .

# Step 4 — Run
hadoop jar myprogram.jar MyClassName /input/path /output/path

# Step 5 — View output
hdfs dfs -cat /output/path/part-r-00000
```

---

---

## 1. WordCount.java — Basic Word Frequency Counter

### What It Does
Reads a text file and counts how many times each word appears. This is the "Hello World" of Hadoop MapReduce.

### Possible Problem Statements
- *"Write a MapReduce program to count the frequency of each word in a given text file."*
- *"Given a large text corpus, find the occurrence count of every word using Hadoop."*

### Core Concept — MapReduce

**MapReduce** is a programming model for processing large datasets in parallel across a cluster.

| Phase | What Happens |
|-------|-------------|
| **Map** | Each line of input is broken into individual words; emits `(word, 1)` for every word |
| **Shuffle & Sort** | Hadoop automatically groups all values by key: `(word, [1, 1, 1, ...])` |
| **Reduce** | Sums up all 1s for each word to produce `(word, total_count)` |

**Flow Diagram:**
```
Input Line: "apple banana apple"
     ↓ Mapper
(apple, 1), (banana, 1), (apple, 1)
     ↓ Shuffle & Sort
apple → [1, 1]
banana → [1]
     ↓ Reducer
apple  2
banana 1
```

**Key Classes Used:**
- `Mapper<Object, Text, Text, IntWritable>` — Input: (offset, line) → Output: (word, 1)
- `Reducer<Text, IntWritable, Text, IntWritable>` — Input: (word, [1,1,...]) → Output: (word, count)
- `StringTokenizer` — splits a line into individual words by whitespace
- `IntWritable` — Hadoop's serializable integer type (like `int` but for HDFS)
- `Text` — Hadoop's serializable string type

### How to Run

```bash
# Create input file
nano words.txt
# Type some text, save with Ctrl+O, exit with Ctrl+X

# Upload to HDFS
hdfs dfs -mkdir /wc
hdfs dfs -put words.txt /wc

# Compile
rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes WordCount.java

# Package
jar -cvf wc.jar -C classes/ .

# Run
hadoop jar wc.jar WordCount /wc/words.txt /wc/output

# View Output
hdfs dfs -cat /wc/output/part-r-00000
```

### Sample Input / Output
```
Input:
hello world hello hadoop world world

Output:
hadoop   1
hello    2
world    3
```

---

---

## 2. MaxNumber.java — Find Maximum Value

### What It Does
Reads a file where each line has one integer, and finds the maximum number using MapReduce.

### Possible Problem Statements
- *"Write a MapReduce program to find the maximum number from a given dataset of integers."*
- *"Using Hadoop MapReduce, find the largest value in a large dataset stored in HDFS."*
- *"Find the maximum sales figure / maximum marks / highest temperature from a dataset using MapReduce."*

### Core Concept — Constant Key Trick

The trick here is that **all mappers emit the same key `"max"`**. This forces all numbers to land in a single reducer, which then finds the global maximum.

```
Input: 10, 45, 2, 99, 76
     ↓ Mapper
(max, 10), (max, 45), (max, 2), (max, 99), (max, 76)
     ↓ Shuffle (all go to one Reducer because key is same)
max → [10, 45, 2, 99, 76]
     ↓ Reducer
max   99
```

**Why `Integer.MIN_VALUE` as initial max?**  
So that any real number in the dataset will be greater than the starting value, guaranteeing correctness even with negative numbers.

### How to Run

```bash
# Create input file (one integer per line)
nano integers.txt
# Type:
# 10
# 45
# 2
# 99
# 76

# Upload
hdfs dfs -mkdir /maxnum
hdfs dfs -put integers.txt /maxnum

# Compile
rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes MaxNumber.java

# Package
jar -cvf maxnum.jar -C classes/ .

# Run
hadoop jar maxnum.jar MaxNumber /maxnum/integers.txt /maxnum/output

# View Output
hdfs dfs -cat /maxnum/output/part-r-00000
```

### Sample Input / Output
```
Input:
10
45
2
99
76

Output:
max   99
```

---

---

## 3. UniqueWords.java — Words Appearing Exactly Once

### What It Does
Finds all words that appear **exactly once** in the entire text. Words repeated more than once are excluded from output.

### Possible Problem Statements
- *"Write a MapReduce program to find all unique words (words that appear only once) in a text file."*
- *"Given a text corpus, identify words that are not repeated anywhere in the document."*
- *"Find words with frequency = 1 using Hadoop MapReduce."*

### Core Concept — Filter in Reducer

Same flow as WordCount, but the Reducer applies a **filter condition**: only emit words where the total count equals exactly 1.

```
Input: "apple banana mango apple orange mango"
     ↓ Mapper (lowercase applied)
(apple,1), (banana,1), (mango,1), (apple,1), (orange,1), (mango,1)
     ↓ Shuffle
apple  → [1, 1]
banana → [1]
mango  → [1, 1]
orange → [1]
     ↓ Reducer (filter: sum == 1)
banana  1
orange  1
```

**Notable Features:**
- Words are converted to **lowercase** before comparison (so "Apple" and "apple" are treated as the same word)
- Uses a **Combiner** (`job.setCombinerClass(UniqueReducer.class)`) — runs a mini-reducer on each mapper's output before sending data across the network, reducing data transfer

> **Combiner Warning:** The combiner here partially sums counts. This is safe for aggregation, but `sum == 1` in the reducer still checks the final global count correctly.

### How to Run

```bash
nano words.txt
# apple banana mango
# apple orange mango
# grapes kiwi orange
# watermelon kiwi

hdfs dfs -mkdir /unique
hdfs dfs -put words.txt /unique

rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes UniqueWords.java

jar -cvf unique.jar -C classes/ .

hadoop jar unique.jar UniqueWords /unique/words.txt /unique/output

hdfs dfs -cat /unique/output/part-r-00000
```

### Sample Input / Output
```
Input:
apple banana mango
apple orange mango
grapes kiwi orange
watermelon kiwi

Output:
banana      1
grapes      1
watermelon  1
```

---

---

## 4. WordCountTopNFreq.java — Top N Most Frequent Words

### What It Does
Finds the **Top N words with the highest frequency** in a text file. N is hardcoded as `5` (you can change it in the code at line `int N = 5`).

### Possible Problem Statements
- *"Write a MapReduce program to find the top 5 most frequently occurring words in a text file."*
- *"Given a large document, find the N most common words using Hadoop."*
- *"Find the top N trending keywords from a dataset using MapReduce."*

### Core Concept — `cleanup()` Method in Reducer

The key technique here is the **`cleanup()` method** — a special hook that runs **once after all `reduce()` calls finish**.

**Why is this needed?**  
The Reducer processes words one at a time. To find Top N, you need to see ALL words first, sort them, then pick the top N. The `cleanup()` method provides this "after everything" hook.

```
Flow:
     ↓ Mapper
Each word → (word, 1)

     ↓ Reducer.reduce() — called for each unique word
wordMap.put("apple", 5)
wordMap.put("banana", 2)
... (stores all words in memory)

     ↓ Reducer.cleanup() — called once at the end
Sort wordMap by value descending
Print top 5 entries
```

**Sorting Logic:**
```java
Collections.sort(list, (a, b) -> b.getValue() - a.getValue()); // descending
```
This lambda sorts entries from highest to lowest frequency.

**Important:** `job.setNumReduceTasks(1)` ensures only **one reducer** runs — necessary because all words must be seen by the same reducer to correctly determine global Top N.

### How to Run

```bash
nano words.txt
# (type a paragraph with varied word frequencies)

hdfs dfs -mkdir /topn
hdfs dfs -put words.txt /topn

rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes WordCountTopNFreq.java

jar -cvf topn.jar -C classes/ .

hadoop jar topn.jar WordCountTopNFreq /topn/words.txt /topn/output

hdfs dfs -cat /topn/output/part-r-00000
```

### To Change N
Open `WordCountTopNFreq.java` and find line:
```java
int N = 5;  // Change this to any number
```

### Sample Output
```
Top 5 Frequent Words   0
1. the                 15
2. is                  12
3. a                   10
4. hadoop              8
5. data                7
```

---

---

## 5. WordCountLeastNFreq.java — Bottom N Least Frequent Words

### What It Does
Finds the **N words with the lowest frequency** — the rarest words in the text. Structurally identical to `WordCountTopNFreq.java` with only the **sort order reversed**.

### Possible Problem Statements
- *"Write a MapReduce program to find the 5 least frequently occurring words in a given text."*
- *"Find the N rarest words in a document using Hadoop MapReduce."*
- *"Identify the bottom N words by frequency from a text corpus."*

### Core Concept — Same as TopN, Just Ascending Sort

The only difference from `WordCountTopNFreq.java` is this one line in `cleanup()`:

| Program | Sort Order | Lambda |
|---------|-----------|--------|
| TopNFreq | Descending (highest first) | `(a, b) -> b.getValue() - a.getValue()` |
| LeastNFreq | Ascending (lowest first) | `(a, b) -> a.getValue() - b.getValue()` |

Everything else — the Mapper, the `cleanup()` pattern, `setNumReduceTasks(1)` — is identical.

### How to Run

```bash
# Same steps as TopNFreq, just change class name

rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes WordCountLeastNFreq.java

jar -cvf leastn.jar -C classes/ .

hadoop jar leastn.jar WordCountLeastNFreq /topn/words.txt /leastn/output

hdfs dfs -cat /leastn/output/part-r-00000
```

### Sample Output
```
Least 5 Frequent Words   0
1. zebra                 1
2. quantum               1
3. xyz                   2
4. mango                 2
5. kiwi                  3
```

---

---

## 6. RelationalAlgebra.java — Database Operations on HDFS

### What It Does
Implements **5 relational algebra operations** from databases — all using Hadoop MapReduce. A single program, pick operation via command-line argument.

| Operation | What It Does |
|-----------|-------------|
| `selection` | Filters rows matching a condition (like SQL `WHERE`) |
| `projection` | Extracts specific columns (like SQL `SELECT col`) |
| `join` | Combines two tables on a common key (like SQL `JOIN`) |
| `intersection` | Rows common to both datasets (like SQL `INTERSECT`) |
| `difference` | Rows in A but not in B (like SQL `EXCEPT`) |

### Possible Problem Statements
- *"Implement Selection and Projection operations on a student dataset using MapReduce."*
- *"Perform a natural join between a Student table and a Marks table using Hadoop."*
- *"Find students common to two lists (Intersection) using MapReduce."*
- *"Using Hadoop, find records present in Set A but not in Set B (Difference)."*

### Core Concepts

#### Selection (WHERE clause)
```
Input (Student.txt):
1,Anushka,CS
2,Rahul,IT
3,Priya,CS

Condition: department == "CS"

Output:
1,Anushka,CS
3,Priya,CS
```
The Mapper checks `fields[2].equals("CS")` and only emits matching rows.

#### Projection (SELECT specific column)
```
Input:
1,Anushka,CS
2,Rahul,IT

Extract only Name (fields[1])

Output:
Anushka
Rahul
```

#### Join (combining two files)
**Key Concept:** Uses `FileSplit` to detect which input file a record comes from. Tags Student records with `"S,"` prefix and Marks records with `"M,"` prefix. The Reducer matches them by student ID.

```
Student.txt: 1,Anushka,CS     → emit (1, S,Anushka,CS)
Marks.txt:   1,90             → emit (1, M,90)

Reducer gets key=1, values=[S,Anushka,CS | M,90]
Output: 1,Anushka,CS,90
```

#### Intersection (common elements)
Mapper emits `(row, 1)` for every row from both files. Reducer counts — if count > 1, the row appeared in more than one file → it's common.

#### Difference (A - B)
Mapper tags each row with its source file ("A" or "B"). Reducer: emit only if `inA == true AND inB == false`.

### Input Files Needed

**Student.txt**
```
1,Anushka,CS
2,Rahul,IT
3,Priya,CS
```

**Marks.txt**
```
1,90
2,85
3,95
```

### How to Run

```bash
# Upload files
hdfs dfs -mkdir /ra
hdfs dfs -put Student.txt /ra
hdfs dfs -put Marks.txt /ra

# Compile
rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes RelationalAlgebra.java

# Package
jar -cvf relational.jar -C classes/ .

# Run Selection (filter CS dept)
hadoop jar relational.jar RelationalAlgebra selection /ra/Student.txt /output/selection
hdfs dfs -cat /output/selection/part-r-00000

# Run Projection (only names)
hdfs dfs -rm -r /output/projection
hadoop jar relational.jar RelationalAlgebra projection /ra/Student.txt /output/projection
hdfs dfs -cat /output/projection/part-r-00000

# Run Join (Student + Marks — comma-separated paths, NO space)
hdfs dfs -rm -r /output/join
hadoop jar relational.jar RelationalAlgebra join /ra/Student.txt,/ra/Marks.txt /output/join
hdfs dfs -cat /output/join/part-r-00000

# Run Intersection (two files with overlapping data)
hdfs dfs -rm -r /output/intersection
hadoop jar relational.jar RelationalAlgebra intersection /ra/Student.txt,/ra/Marks.txt /output/intersection
hdfs dfs -cat /output/intersection/part-r-00000

# Run Difference (A - B)
hdfs dfs -rm -r /output/difference
hadoop jar relational.jar RelationalAlgebra difference /ra/A.txt,/ra/B.txt /output/difference
hdfs dfs -cat /output/difference/part-r-00000
```

> **Important for Join/Difference:** Input paths are **comma-separated with NO spaces** — `path1,path2`

### Expected Outputs

**Selection (CS department):**
```
1,Anushka,CS
3,Priya,CS
```

**Projection (names only):**
```
Anushka
Priya
Rahul
```

**Join:**
```
1,Anushka,CS,90
2,Rahul,IT,85
3,Priya,CS,95
```

---

---

## 7. GenericDatasetAnalysis.java — CSV Dataset Statistics (3-Job Chain)

### What It Does
Runs **3 MapReduce jobs sequentially** on any CSV dataset to produce:
1. **Job 1 (Count):** How many records exist per category
2. **Job 2 (Sum):** Total sum of a numeric column per category
3. **Job 3 (Stats):** Max, Min, and Average of a numeric column per category

Designed to work with almost any CSV — just change the column index numbers.

### Possible Problem Statements
- *"Write a MapReduce program to compute count, sum, and statistics (max, min, average) on a sales dataset grouped by product category."*
- *"Given a CSV dataset of student marks, calculate total marks, average, highest and lowest marks per department using Hadoop."*
- *"Perform dataset analysis (frequency count, sum, max/min/avg) on an employee dataset using chained MapReduce jobs."*

### Core Concept — Chained MapReduce Jobs

Three separate `Job` objects are created and run **sequentially** (one after another):
```
job1.waitForCompletion(true);   ← waits for Job 1 to finish
job2.waitForCompletion(true);   ← then runs Job 2
job3.waitForCompletion(true);   ← then runs Job 3
```
Each job reads the same input file but writes to a different output directory.

**Header Skipping:**
```java
if (key.get() == 0 && line.contains("ID")) return;
```
The first line (offset 0) containing "ID" is treated as the header and skipped.

**How to Adapt to Any Dataset:**
```
Dataset columns: ID, Product, Region, Sales
Index:           0    1        2       3

To group by Product and analyze Sales:
  String category = fields[1].trim();      // Product
  double number = Double.parseDouble(fields[3].trim()); // Sales
```

### Sample Dataset (`dataset.csv`)

```
ID,Product,Region,Sales
1,Laptop,North,50000
2,Mobile,South,20000
3,Laptop,East,45000
4,Tablet,West,15000
5,Mobile,North,22000
6,Laptop,South,52000
7,Tablet,East,18000
8,Mobile,West,25000
9,Laptop,North,48000
10,Tablet,South,17000
```

### How to Run

```bash
# Create and upload dataset
nano dataset.csv
# (paste the sample data above)

hdfs dfs -mkdir /generic
hdfs dfs -put dataset.csv /generic

# Compile
rm -rf classes && mkdir classes
javac -classpath $(hadoop classpath) -d classes GenericDatasetAnalysis.java

# Package
jar -cvf generic.jar -C classes/ .

# Run (4 arguments: input + 3 output paths)
hadoop jar generic.jar GenericDatasetAnalysis \
  /generic/dataset.csv \
  /generic/output1 \
  /generic/output2 \
  /generic/output3

# View Count output (Job 1)
hdfs dfs -cat /generic/output1/part-r-00000

# View Sum output (Job 2)
hdfs dfs -cat /generic/output2/part-r-00000

# View Stats output (Job 3)
hdfs dfs -cat /generic/output3/part-r-00000

# If output dirs already exist, delete them first
hdfs dfs -rm -r /generic/output1
hdfs dfs -rm -r /generic/output2
hdfs dfs -rm -r /generic/output3
```

### Expected Output

**Count (output1):**
```
Laptop   4.0
Mobile   3.0
Tablet   3.0
```

**Sum (output2):**
```
Laptop   195000.0
Mobile   67000.0
Tablet   50000.0
```

**Stats (output3):**
```
Laptop   Max=52000.0 Min=45000.0 Avg=48750.0
Mobile   Max=25000.0 Min=20000.0 Avg=22333.33
Tablet   Max=18000.0 Min=15000.0 Avg=16666.67
```

---

---

## Quick Reference — All Programs

| File | Problem | Input | Key Concept |
|------|---------|-------|-------------|
| `WordCount.java` | Count word frequency | Text file | Basic MapReduce |
| `MaxNumber.java` | Find maximum number | One integer per line | Constant key trick |
| `UniqueWords.java` | Words appearing once | Text file | Filter in Reducer (`sum == 1`) |
| `WordCountTopNFreq.java` | Top N frequent words | Text file | `cleanup()` method + sort descending |
| `WordCountLeastNFreq.java` | Bottom N rare words | Text file | `cleanup()` method + sort ascending |
| `RelationalAlgebra.java` | DB ops (SELECT/JOIN/etc.) | CSV tables | `FileSplit` for file detection |
| `GenericDatasetAnalysis.java` | Count/Sum/Stats on CSV | CSV dataset | 3 chained jobs |

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `Output directory already exists` | Rerunning without deleting output | `hdfs dfs -rm -r /output` |
| `ClassNotFoundException` | Wrong class name in `hadoop jar` command | Match exact class name from `.java` file |
| `NumberFormatException` | Non-numeric value in numeric column | Check dataset for headers or bad rows |
| `ArrayIndexOutOfBoundsException` | Wrong `fields[index]` | Count CSV columns from 0 |
| `Connection refused` / services not running | Hadoop not started | `start-dfs.sh && start-yarn.sh` |
| `SafeModeException` | NameNode in safe mode | `hdfs dfsadmin -safemode leave` |

---

## Key MapReduce Concepts to Remember

- **Mapper** — transforms input records into key-value pairs
- **Shuffle & Sort** — Hadoop's automatic grouping of values by key (you don't write this)
- **Reducer** — aggregates all values for each key into a final result
- **Combiner** — optional mini-reducer that runs on mapper output to reduce network traffic
- **`cleanup()`** — a Reducer method that runs once after all reduce() calls; used for Top-N patterns
- **`FileSplit`** — lets the mapper know which input file a record came from; used in Join and Difference
- **`NullWritable`** — used as value when you only care about the key (no value needed in output)
- **`setNumReduceTasks(1)`** — forces all data to one reducer; required when global sorting/ranking is needed
- **`IntWritable` / `Text` / `DoubleWritable`** — Hadoop's serializable wrappers for Java's `int`, `String`, `double`
