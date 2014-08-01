package mapred_hbase_fill_table;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MySummaryFileJob {

    public static class MyMapper extends TableMapper<Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value,
                Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException,
                InterruptedException {
            String val = new String(value.getValue(Bytes.toBytes("Name"), Bytes.toBytes("col2")));
            text.set(val);
            context.write(text, ONE);
        }

    }

    // Word Count reducer
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

}
