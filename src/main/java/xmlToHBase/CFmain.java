package xmlToHBase;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CFmain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CFmain(), args));
    }

    public static String parseISO8601(long date) {
        return String.valueOf(new SimpleDateFormat("mm:ss").format(date));
    }

    @Override
    public int run(String[] args) throws Exception {
        // final long starttime = System.currentTimeMillis();

        final Configuration conf = HBaseConfiguration.create();

        // <-------- configuration needed for running the application from Eclipse ---------->
        
        /*
                conf.addResource(new Path("/home/funnyboy/hadoop-1.2.1/conf/core-site.xml"));
                conf.addResource(new Path("/home/funnyboy/hadoop-1.2.1/conf/hdfs-site.xml"));
                conf.addResource(new Path("/home/funnyboy/hbase-0.94.8/conf/hbase-site.xml"));
        */
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");
        /*
                conf.set("fs.default.name", "hdfs://localhost:9000");
                conf.set("mapred.job.tracker", "localhost:9001");
                conf.set("mapred.jar", "/home/funnyboy/Downloads/smallfiles.jar");
        */
        // System.out.println("everything works from Eclipse");

        conf.set(CFInputFormat.START_TAG_KEY, "<fits");
        conf.set(CFInputFormat.END_TAG_KEY, "</fits>");

        final Job job = new Job(conf, "small files");
        job.setJarByClass(CFmain.class);
        job.setInputFormatClass(CFInputFormat.class);
        job.setMapperClass(CFMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), HTableInterface.class);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), HTable.class);

        final String input = args[0];
        final String output = args[1];

        // System.out.println("input: " + input);
        // System.out.println("output: " + output);

        FileInputFormat.setInputPaths(job, new Path(input));
        final Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);

        outPath.getFileSystem(conf).delete(outPath, true);
        job.waitForCompletion(true);

        // final long endtime = System.currentTimeMillis();
        // System.out.println("runtime: " + parseISO8601(endtime - starttime));

        return 0;
    }

}
