package mapred_wordcount_10;

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

    public static class TokenCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        private final Text word;

        public TokenCounterMapper() {
            word = new Text();
        }

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            for (StringTokenizer itr = new StringTokenizer(value.toString()); itr.hasMoreTokens();) {
                word.set(itr.nextToken());
                context.write(word, one);
                // System.out.println("word: " + word + " one: " + one);
            }
        }
    }

    public static class TokenCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            // System.out.println("key: " + key + " sum: " + sum);
        }

    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        // configuration needed for running the application from Eclipse
        conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/hdfs-site.xml"));
        // conf.set("fs.default.name", "hdfs://localhost:8020");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenCounterMapper.class);
        job.setReducerClass(TokenCounterReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path[] { new Path("/mapred_wordcount_10/ArcheryRules.txt") });
        Path outPath = new Path("/mapred_wordcount_10/outputs");
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
