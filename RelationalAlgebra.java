import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelationalAlgebra {

    // =========================================
    // SELECTION
    // =========================================

    public static class SelectionMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            // Select only CS department
            if (fields.length == 3 &&
                    fields[2].equals("CS")) {

                context.write(value, NullWritable.get());
            }
        }
    }

    public static class SelectionReducer
            extends Reducer<Text, NullWritable,
            Text, NullWritable> {

        public void reduce(Text key,
                           Iterable<NullWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    // =========================================
    // PROJECTION
    // =========================================

    public static class ProjectionMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            // Print only Name column
            if (fields.length >= 2) {

                context.write(
                        new Text(fields[1]),
                        NullWritable.get()
                );
            }
        }
    }

    public static class ProjectionReducer
            extends Reducer<Text, NullWritable,
            Text, NullWritable> {

        public void reduce(Text key,
                           Iterable<NullWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    // =========================================
    // JOIN
    // =========================================

    public static class JoinMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String fileName =
                    ((FileSplit) context.getInputSplit())
                            .getPath()
                            .getName();

            String[] fields =
                    value.toString().split(",");

            // Student file
            if (fileName.contains("Student")) {

                // key = student id
                // value = S,name,dept
                context.write(
                        new Text(fields[0]),
                        new Text("S," +
                                fields[1] + "," +
                                fields[2])
                );

            } else {

                // Marks file
                // key = student id
                // value = M,marks
                context.write(
                        new Text(fields[0]),
                        new Text("M," +
                                fields[1])
                );
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text,
            Text, NullWritable> {

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            String studentData = "";
            String marks = "";

            for (Text val : values) {

                String[] parts =
                        val.toString().split(",");

                // Student record
                if (parts[0].equals("S")) {

                    studentData =
                            parts[1] + "," + parts[2];

                } else {

                    // Marks record
                    marks = parts[1];
                }
            }

            // Final joined output
            context.write(
                    new Text(
                            key.toString() + "," +
                                    studentData + "," +
                                    marks
                    ),
                    NullWritable.get()
            );
        }
    }

    // =========================================
    // INTERSECTION
    // =========================================

    public static class IntersectionMapper
            extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            context.write(value, one);
        }
    }

    public static class IntersectionReducer
            extends Reducer<Text, IntWritable,
            Text, NullWritable> {

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable val : values) {
                count++;
            }

            // Common in both files
            if (count > 1) {

                context.write(
                        key,
                        NullWritable.get()
                );
            }
        }
    }

    // =========================================
    // DIFFERENCE (A - B)
    // =========================================

    public static class DifferenceMapper
            extends Mapper<LongWritable, Text,
            Text, Text> {

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String fileName =
                    ((FileSplit) context.getInputSplit())
                            .getPath()
                            .getName();

            if (fileName.contains("A")) {

                context.write(
                        value,
                        new Text("A")
                );

            } else {

                context.write(
                        value,
                        new Text("B")
                );
            }
        }
    }

    public static class DifferenceReducer
            extends Reducer<Text, Text,
            Text, NullWritable> {

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            boolean inA = false;
            boolean inB = false;

            for (Text val : values) {

                if (val.toString().equals("A"))
                    inA = true;

                if (val.toString().equals("B"))
                    inB = true;
            }

            // A - B
            if (inA && !inB) {

                context.write(
                        key,
                        NullWritable.get()
                );
            }
        }
    }

    // =========================================
    // MAIN DRIVER
    // =========================================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Relational Algebra"
        );

        job.setJarByClass(
                RelationalAlgebra.class
        );

        String operation = args[0];

        switch (operation) {

            // =====================
            // SELECTION
            // =====================

            case "selection":

                job.setMapperClass(
                        SelectionMapper.class
                );

                job.setReducerClass(
                        SelectionReducer.class
                );

                job.setOutputKeyClass(
                        Text.class
                );

                job.setOutputValueClass(
                        NullWritable.class
                );

                break;

            // =====================
            // PROJECTION
            // =====================

            case "projection":

                job.setMapperClass(
                        ProjectionMapper.class
                );

                job.setReducerClass(
                        ProjectionReducer.class
                );

                job.setOutputKeyClass(
                        Text.class
                );

                job.setOutputValueClass(
                        NullWritable.class
                );

                break;

            // =====================
            // JOIN
            // =====================

            case "join":

                job.setMapperClass(
                        JoinMapper.class
                );

                job.setReducerClass(
                        JoinReducer.class
                );

                job.setOutputKeyClass(
                        Text.class
                );

                job.setOutputValueClass(
                        Text.class
                );

                break;

            // =====================
            // INTERSECTION
            // =====================

            case "intersection":

                job.setMapperClass(
                        IntersectionMapper.class
                );

                job.setReducerClass(
                        IntersectionReducer.class
                );

                job.setOutputKeyClass(
                        Text.class
                );

                job.setOutputValueClass(
                        IntWritable.class
                );

                break;

            // =====================
            // DIFFERENCE
            // =====================

            case "difference":

                job.setMapperClass(
                        DifferenceMapper.class
                );

                job.setReducerClass(
                        DifferenceReducer.class
                );

                job.setOutputKeyClass(
                        Text.class
                );

                job.setOutputValueClass(
                        Text.class
                );

                break;

            default:

                System.out.println(
                        "Invalid operation"
                );

                System.exit(1);
        }

        // IMPORTANT
        // For JOIN / DIFFERENCE
        // give comma separated paths

        FileInputFormat.addInputPaths(
                job,
                args[1]
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[2])
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

2. Create Input Files

nano Student.txt
Content:
1,Anushka,CS
2,Rahul,IT
3,Priya,CS

nano Marks.txt
Content:
1,90
2,85
3,95

(For Intersection/Difference, create two sets)
nano A.txt
Content:
Alice
Bob
Charlie

nano B.txt
Content:
Bob
Charlie
David

3. Create HDFS Directory and Upload

hdfs dfs -mkdir /ra
hdfs dfs -put Student.txt /ra
hdfs dfs -put Marks.txt /ra
hdfs dfs -put A.txt /ra
hdfs dfs -put B.txt /ra

4. Compile

rm -rf classes
mkdir classes
javac -classpath $(hadoop classpath) -d classes RelationalAlgebra.java

5. Create JAR

jar -cvf relational.jar -C classes/ .

6. Run SELECTION (filter rows — CS department)

hadoop jar relational.jar RelationalAlgebra selection /ra/Student.txt /output/selection
hdfs dfs -cat /output/selection/part-r-00000

7. Run PROJECTION (extract one column — Name only)

hdfs dfs -rm -r /output/projection
hadoop jar relational.jar RelationalAlgebra projection /ra/Student.txt /output/projection
hdfs dfs -cat /output/projection/part-r-00000

8. Run JOIN (Student + Marks on Student ID)
NOTE: Two input paths separated by comma, NO spaces

hdfs dfs -rm -r /output/join
hadoop jar relational.jar RelationalAlgebra join /ra/Student.txt,/ra/Marks.txt /output/join
hdfs dfs -cat /output/join/part-r-00000

9. Run INTERSECTION (common rows in both files)

hdfs dfs -rm -r /output/intersection
hadoop jar relational.jar RelationalAlgebra intersection /ra/A.txt,/ra/B.txt /output/intersection
hdfs dfs -cat /output/intersection/part-r-00000

10. Run DIFFERENCE (rows in A but NOT in B)

hdfs dfs -rm -r /output/difference
hadoop jar relational.jar RelationalAlgebra difference /ra/A.txt,/ra/B.txt /output/difference
hdfs dfs -cat /output/difference/part-r-00000

11. Stop Hadoop (Optional)

stop-dfs.sh
stop-yarn.sh

*/