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

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
(type some text, Ctrl+O to save, Ctrl+X to exit)

3. Create HDFS Directory and Upload

hdfs dfs -mkdir /wc
hdfs dfs -put words.txt /wc
hdfs dfs -ls /wc

4. Compile

rm -rf classes
mkdir classes
javac -classpath $(hadoop classpath) -d classes WordCount.java

5. Create JAR

jar -cvf wc.jar -C classes/ .

6. Run Program

hadoop jar wc.jar WordCount /wc/words.txt /wc/output

7. View Output

hdfs dfs -cat /wc/output/part-r-00000

8. If Output Directory Already Exists

hdfs dfs -rm -r /wc/output
hadoop jar wc.jar WordCount /wc/words.txt /wc/output

9. Stop Hadoop (Optional)

stop-dfs.sh
stop-yarn.sh

*/