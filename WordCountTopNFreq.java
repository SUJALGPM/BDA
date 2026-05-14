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

public class WordCountTopNFreq {
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
        // PRINT TOP N WORDS
        // =========================

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // CHANGE N HERE
            int N = 5;

            // Convert map to list
            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(wordMap.entrySet());

            // Sort descending by frequency
            Collections.sort(
                    list,
                    (a, b) -> b.getValue() - a.getValue()
            );

            context.write(
                    new Text("Top " + N + " Frequent Words"),
                    new IntWritable(0)
            );

            // Print Top N Words
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
                "Top N Frequent Words"
        );

        job.setJarByClass(
                WordCountTopNFreq.class
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

prerequisties

in vs terminal:-
java -version
8 should come

echo $JAVA_HOME
8 should come

if no:
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

and set 8

hadoop version


start-dfs.sh 
start-yarn.sh

jps
should see everything

if not working:
stop-all.sh
start-all.sh

still broken:
hdfs namenode -format
start-all.sh

leave safe mode:
hdfs dfsadmin -safemode leave


now 


write program on vs code
save as WordCountTopNFreq.java
Desktop->BDA

cd Desktop
cd BDA

nano words.txt

hdfs dfs -mkdir /exp4

hdfs dfs -ls /

hdfs dfs -put words.txt /exp4

# VERIFY FILE
hdfs dfs -ls /exp4

rm -rf classes

mkdir classes

javac -classpath $(hadoop classpath) -d classes WordCountTopNFreq.java

# CHECK CLASS FILES
ls classes

# =========================
# CREATE JAR
# =========================

jar -cvf exp4.jar -C classes/ .

# CHECK JAR EXISTS
ls

# =========================
# RUN MAPREDUCE
# =========================

hadoop jar exp4.jar WordCountTopNFreq /exp4/words.txt /output/wordcount

# =========================
# VIEW OUTPUT
# =========================

hdfs dfs -cat /output/wordcount/part-r-00000

# =========================
# IF OUTPUT ALREADY EXISTS
# =========================

hdfs dfs -rm -r /output

# RUN AGAIN
hadoop jar exp4.jar WordCountTopNFreq /exp4/words.txt /output/wordcount

# =========================
# CHECK HDFS FILES
# =========================

hdfs dfs -ls /output/wordcount

# =========================
# STOP HADOOP (OPTIONAL)
# =========================

stop-dfs.sh

stop-yarn.sh

*/
/* 
echo $HADOOP_HOME coming empty means the Hadoop environment variable is not set in your terminal session.
You need to set it manually in your shell config file (~/.zshrc on Mac usually).
Example:
nano ~/.zshrc
Add these lines (change path according to your Hadoop install location):
export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.4.1/libexec
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
Then save:
CTRL + O → Enter
CTRL + X
Apply changes:
source ~/.zshrc
Now check:
echo $HADOOP_HOME
It should print

/opt/homebrew/Cellar/hadoop/3.4.1/libexec*/

/*
Find Java 8 Path

Run:

sudo update-alternatives --config java

You will see paths like:

/usr/lib/jvm/java-8-openjdk-amd64/bin/java

Copy the Java 8 path without /bin/java:

/usr/lib/jvm/java-8-openjdk-amd64
Set JAVA_HOME

Open Hadoop environment file:

nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

Find:

export JAVA_HOME=

Replace with:

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

*/