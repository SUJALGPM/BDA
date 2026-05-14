import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenericDatasetAnalysis {

    // ==================================
    // JOB 1 : COUNT CATEGORY FREQUENCY
    // ==================================

    public static class CountMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip Header
            if (key.get() == 0 &&
                    line.contains("ID")) {
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE THIS COLUMN INDEX
                String category = fields[1].trim();

                context.write(
                        new Text(category),
                        new DoubleWritable(1)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class CountReducer
            extends Reducer<Text, DoubleWritable,
            Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            context.write(
                    key,
                    new DoubleWritable(sum)
            );
        }
    }

    // ================================
    // JOB 2 : SUM NUMERIC COLUMN
    // ================================

    public static class SumMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip Header
            if (key.get() == 0 &&
                    line.contains("ID")) {
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE CATEGORY COLUMN INDEX
                String category = fields[1].trim();

                // CHANGE NUMERIC COLUMN INDEX
                double number =
                        Double.parseDouble(fields[3].trim());

                context.write(
                        new Text(category),
                        new DoubleWritable(number)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, DoubleWritable,
            Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double total = 0;

            for (DoubleWritable val : values) {
                total += val.get();
            }

            context.write(
                    key,
                    new DoubleWritable(total)
            );
        }
    }

    // ================================
    // JOB 3 : MAX MIN AVG
    // ================================

    public static class StatsMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip Header
            if (key.get() == 0 &&
                    line.contains("ID")) {
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE CATEGORY COLUMN INDEX
                String category = fields[1].trim();

                // CHANGE NUMERIC COLUMN INDEX
                double num =
                        Double.parseDouble(fields[3].trim());

                context.write(
                        new Text(category),
                        new DoubleWritable(num)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class StatsReducer
            extends Reducer<Text, DoubleWritable,
            Text, Text> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            // CORRECTED MAX INITIALIZATION
            double max = Double.NEGATIVE_INFINITY;
            double min = Double.MAX_VALUE;

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {

                double num = val.get();

                if (num > max)
                    max = num;

                if (num < min)
                    min = num;

                sum += num;
                count++;
            }

            double avg = sum / count;

            context.write(
                    key,
                    new Text(
                            "Max=" + max +
                            " Min=" + min +
                            " Avg=" + avg
                    )
            );
        }
    }

    // ================================
    // MAIN FUNCTION
    // ================================

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        // ======================
        // JOB 1 : COUNT
        // ======================

        Job job1 = Job.getInstance(conf, "Count");

        job1.setJarByClass(GenericDatasetAnalysis.class);

        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(CountReducer.class);

        // Combiner for optimization
        job1.setCombinerClass(CountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job1,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job1,
                new Path(args[1])
        );

        job1.waitForCompletion(true);

        // ======================
        // JOB 2 : SUM
        // ======================

        Job job2 = Job.getInstance(conf, "Sum");

        job2.setJarByClass(GenericDatasetAnalysis.class);

        job2.setMapperClass(SumMapper.class);
        job2.setReducerClass(SumReducer.class);

        // Combiner for optimization
        job2.setCombinerClass(SumReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job2,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job2,
                new Path(args[2])
        );

        job2.waitForCompletion(true);

        // ======================
        // JOB 3 : STATS
        // ======================

        Job job3 = Job.getInstance(conf, "Stats");

        job3.setJarByClass(GenericDatasetAnalysis.class);

        job3.setMapperClass(StatsMapper.class);
        job3.setReducerClass(StatsReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(DoubleWritable.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job3,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job3,
                new Path(args[3])
        );

        System.exit(
                job3.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}
/*

==================================================
SAMPLE DATASET
==================================================

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

==================================================
WHAT THIS PROGRAM DOES
==================================================

1. Counts category frequency
2. Sums numeric values
3. Calculates Max Min Avg

Works for almost any CSV dataset
by changing only column indexes.

==================================================
HOW TO CHANGE FOR ANY DATASET
==================================================

Suppose Dataset Columns Are:

ID,Product,Region,Sales

Then:

fields[1] = Product
fields[2] = Region
fields[3] = Sales

Change ONLY these lines:

---------------------------------

String category = fields[1].trim();

---------------------------------

double number =
Double.parseDouble(fields[3].trim());

---------------------------------

You can replace:
fields[1]
fields[3]

with any dataset column indexes.

Examples:

fields[0] = ID
fields[1] = Product
fields[2] = Region
fields[3] = Sales

==================================================
HOW TO EXECUTE
==================================================

1. START HADOOP

start-dfs.sh
start-yarn.sh

CHECK SERVICES:

jps

You should see:

NameNode
DataNode
ResourceManager
NodeManager
SecondaryNameNode

==================================================

2. CREATE HDFS DIRECTORY

hdfs dfs -mkdir /generic

CHECK:

hdfs dfs -ls /

==================================================

3. PUT DATASET INTO HDFS

hdfs dfs -put dataset.csv /generic

VERIFY:

hdfs dfs -ls /generic

==================================================

4. COMPILE JAVA FILE

REMOVE OLD CLASSES:

rm -rf classes

CREATE NEW CLASSES FOLDER:

mkdir classes

COMPILE:

javac -classpath $(hadoop classpath) \
-d classes GenericDatasetAnalysis.java

==================================================

5. CREATE JAR FILE

jar -cvf generic.jar -C classes/ .

==================================================

6. RUN PROGRAM

hadoop jar generic.jar GenericDatasetAnalysis \
/generic/dataset.csv \
/generic/output1 \
/generic/output2 \
/generic/output3

==================================================

7. VIEW OUTPUTS

---------------------------------
COUNT OUTPUT
---------------------------------

hdfs dfs -cat /generic/output1/part-r-00000

---------------------------------
SUM OUTPUT
---------------------------------

hdfs dfs -cat /generic/output2/part-r-00000

---------------------------------
MAX MIN AVG OUTPUT
---------------------------------

hdfs dfs -cat /generic/output3/part-r-00000

==================================================
EXPECTED OUTPUT
==================================================

COUNT OUTPUT:

Laptop  4.0
Mobile  3.0
Tablet  3.0

---------------------------------

SUM OUTPUT:

Laptop  195000.0
Mobile  67000.0
Tablet  50000.0

---------------------------------

MAX MIN AVG OUTPUT:

Laptop Max=52000.0 Min=45000.0 Avg=48750.0

Mobile Max=25000.0 Min=20000.0 Avg=22333.333333333332

Tablet Max=18000.0 Min=15000.0 Avg=16666.666666666668

==================================================
IMPORTANT NOTES
==================================================

1. INPUT FILE MUST BE CSV

2. HEADER SHOULD EXIST

3. CHANGE COLUMN INDEXES CAREFULLY

4. THIS PROGRAM WORKS FOR:
   - Sales datasets
   - Student datasets
   - Employee datasets
   - Product datasets
   - Almost any CSV dataset

5. IF OUTPUT DIRECTORY EXISTS:

RUN:

hdfs dfs -rm -r /generic/output1
hdfs dfs -rm -r /generic/output2
hdfs dfs -rm -r /generic/output3

Then rerun the program.

==================================================
COMMON ERRORS
==================================================

1. ClassNotFoundException
-> Wrong class name in hadoop jar command

2. Output directory already exists
-> Delete output folders first

3. NumberFormatException
-> Numeric column contains text

4. ArrayIndexOutOfBoundsException
-> Wrong column index

5. Hadoop services not running
-> Start dfs and yarn

==================================================

A tiny attempt to make Hadoop less annoying.
Only partially successful.

==================================================

*/