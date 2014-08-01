package mapred_hbase_fill_table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRedOnHBase {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();

        // configuration needed for running the application from Eclipse
        conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml"));

        Job job = Job.getInstance(conf);
        // job.setJarByClass(MySummaryFileJob.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        String sourceTable = "User3";
        TableMapReduceUtil.initTableMapperJob(sourceTable, scan, MySummaryFileJob.MyMapper.class, Text.class,
                IntWritable.class, job);
        job.setReducerClass(MySummaryFileJob.MyReducer.class);
        job.setNumReduceTasks(1);
        Path outPath = new Path("/mapred_hbase_fill_table/mySummaryFile");
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        if (!job.waitForCompletion(true)) {
            throw new IOException("error with job!");
        }
    }
}
