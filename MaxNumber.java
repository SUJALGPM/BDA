import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxNumber {

    public static class MaxMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            int number = Integer.parseInt(value.toString());
            context.write(new Text("max"), new IntWritable(number));
        }
    }

    public static class MaxReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;

            for (IntWritable val : values) {
                if (val.get() > max) {
                    max = val.get();
                }
            }

            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "max number");

        job.setJarByClass(MaxNumber.class);
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);

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

2. Create HDFS Directory

hdfs dfs -mkdir /exp4

3. Put Dataset
Create:
nano integers.txt
Put:
10
45
2
99
76
15
34
120
87
hdfs dfs -put integers.txt /exp4

4. Compile

mkdir classes

javac -classpath $(hadoop classpath) -d classes MaxNumber.java

5. Create JAR

jar -cvf exp4.jar -C classes/ .

6. Run Program

hadoop jar exp4.jar MaxNumber /exp4/integers.txt /output/max

7. View Output

hdfs dfs -cat /output/max/part-r-00000

*/