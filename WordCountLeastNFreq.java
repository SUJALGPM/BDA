import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountLeastNFreq {

    // =========================
    // MAPPER
    // =========================

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        private Text word = new Text();

        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr =
                    new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                word.set(
                        itr.nextToken().toLowerCase()
                );

                context.write(word, one);
            }
        }
    }

    // =========================
    // REDUCER
    // =========================

    public static class IntSumReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        // Store all word frequencies
        private Map<String, Integer> wordMap =
                new HashMap<>();

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();
            }

            // Store word and frequency
            wordMap.put(key.toString(), sum);
        }

        // =========================
        // PRINT LEAST N WORDS
        // =========================

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // CHANGE N HERE
            int N = 5;

            // Convert map to list
            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(wordMap.entrySet());

            // Sort ascending by frequency
            Collections.sort(
                    list,
                    (a, b) -> a.getValue() - b.getValue()
            );

            context.write(
                    new Text("Least " + N + " Frequent Words"),
                    new IntWritable(0)
            );

            // Print Least N Words
            for (int i = 0;
                 i < N && i < list.size();
                 i++) {

                Map.Entry<String, Integer> entry =
                        list.get(i);

                context.write(
                        new Text(
                                (i + 1) + ". " +
                                        entry.getKey()
                        ),
                        new IntWritable(
                                entry.getValue()
                        )
                );
            }
        }
    }

    // =========================
    // DRIVER CODE
    // =========================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Least N Frequent Words"
        );

        job.setJarByClass(
                WordCountLeastNFreq.class
        );

        job.setMapperClass(
                TokenizerMapper.class
        );

        job.setReducerClass(
                IntSumReducer.class
        );
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);

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

=========== EXECUTION COMMANDS ===========

1. Start Hadoop

start-dfs.sh
start-yarn.sh
jps

2. Create Input File

nano words.txt
(type some text with repeated words, Ctrl+O to save, Ctrl+X to exit)

3. Create HDFS Directory and Upload

hdfs dfs -mkdir /leastn
hdfs dfs -put words.txt /leastn
hdfs dfs -ls /leastn

4. Compile

rm -rf classes
mkdir classes
javac -classpath $(hadoop classpath) -d classes WordCountLeastNFreq.java

5. Create JAR

jar -cvf leastn.jar -C classes/ .

6. Run Program

hadoop jar leastn.jar WordCountLeastNFreq /leastn/words.txt /leastn/output

7. View Output

hdfs dfs -cat /leastn/output/part-r-00000

8. If Output Directory Already Exists

hdfs dfs -rm -r /leastn/output
hadoop jar leastn.jar WordCountLeastNFreq /leastn/words.txt /leastn/output

9. Stop Hadoop (Optional)

stop-dfs.sh
stop-yarn.sh

*/