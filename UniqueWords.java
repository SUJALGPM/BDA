import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueWords {

    // ==================================
    // MAPPER
    // ==================================

    public static class UniqueMapper
            extends Mapper<Object, Text,
            Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        private Text word = new Text();

        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr =
                    new StringTokenizer(
                            value.toString()
                    );

            while (itr.hasMoreTokens()) {

                word.set(
                        itr.nextToken()
                                .toLowerCase()
                );

                context.write(word, one);
            }
        }
    }

    // ==================================
    // REDUCER
    // ==================================

    public static class UniqueReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            // PRINT ONLY UNIQUE WORDS
            // frequency = 1

            if (sum == 1) {

                context.write(
                        key,
                        new IntWritable(sum)
                );
            }
        }
    }

    // ==================================
    // MAIN FUNCTION
    // ==================================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Unique Words"
        );

        job.setJarByClass(
                UniqueWords.class
        );

        job.setMapperClass(
                UniqueMapper.class
        );

        job.setReducerClass(
                UniqueReducer.class
        );

        // COMBINER FOR OPTIMIZATION

        job.setCombinerClass(
                UniqueReducer.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                IntWritable.class
        );

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}

/*

==================================================
SAMPLE INPUT
==================================================

apple banana mango
apple orange mango
grapes kiwi orange
watermelon kiwi

==================================================
EXPECTED OUTPUT
==================================================

banana      1
grapes      1
watermelon  1

Only words appearing exactly once
are printed.

==================================================
HOW TO EXECUTE
==================================================

1. START HADOOP

start-dfs.sh
start-yarn.sh

CHECK:

jps

==================================================

2. CREATE HDFS DIRECTORY

hdfs dfs -mkdir /unique

==================================================

3. PUT INPUT FILE

hdfs dfs -put words.txt /unique

==================================================

4. COMPILE JAVA FILE

rm -rf classes

mkdir classes

javac -classpath $(hadoop classpath) \
-d classes UniqueWords.java

==================================================

5. CREATE JAR FILE

jar -cvf unique.jar -C classes/ .

==================================================

6. RUN PROGRAM

hadoop jar unique.jar UniqueWords \
/unique/words.txt \
/unique/output

==================================================

7. VIEW OUTPUT

hdfs dfs -cat /unique/output/part-r-00000

==================================================
IMPORTANT NOTES
==================================================

1. Converts words to lowercase

2. Removes duplicate occurrences

3. Prints ONLY unique words

4. If output folder exists:

hdfs dfs -rm -r /unique/output

==================================================

COMMON ERRORS
==================================================

1. Output directory already exists
-> Delete old output folder

2. ClassNotFoundException
-> Wrong class name in command

3. Hadoop services not running
-> Start dfs and yarn

==================================================

*/