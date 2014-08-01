package mapred_mahout_xml_parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

public final class CommonTableDriver {

    public static void commonRunJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();

        conf.set("xmlinput.start", "<property>");
        conf.set("xmlinput.end", "</property>");

        Job job = Job.getInstance(conf);
        // job.setJarByClass(CommonTableDriver.class);
        job.setInputFormatClass(XmlInputFormat.class); 
        job.setMapperClass(CommonTableMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path[] { new Path(input) });
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        commonRunJob(args[0], args[1]);
    }

}
