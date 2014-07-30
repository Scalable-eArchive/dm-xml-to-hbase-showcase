package smallfiles;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Reads XML and extracts the relevant part.
 */
public class CFRecordReader extends RecordReader<FileLineWritable, Text> {

    private static final String ENCODING = "utf-8";
    
    private final byte[] xmlStartTag; 
    private final byte[] xmlEndTag;

    private final long startOffset;
    private final long end;
    private final long pos;
    private final FileSystem fs;
    private FileLineWritable key;
    private Text value;

    private final FSDataInputStream fileIn;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    public CFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        xmlStartTag = context.getConfiguration().get(CFInputFormat.START_TAG_KEY).getBytes(ENCODING);
        xmlEndTag = context.getConfiguration().get(CFInputFormat.END_TAG_KEY).getBytes(ENCODING);

        Path path = split.getPath(index);
        fs = path.getFileSystem(context.getConfiguration());
        
        startOffset = split.getOffset(index);
        end = startOffset + split.getLength(index);

        fileIn = fs.open(path);
        pos = startOffset;

        fileIn.seek(startOffset);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
    }

    @Override
    public void close() throws IOException {
        fileIn.close();
    }

    @Override
    public float getProgress() {
        if (startOffset == end) {
            return 0;
        }
        return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
    }

    @Override
    public FileLineWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    public DataOutputBuffer getBuffer() {
        return buffer;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (fileIn.getPos() < end) {
            if (readUntilMatch(xmlStartTag, false)) {
                try {
                    buffer.write(xmlStartTag);
                    if (readUntilMatch(xmlEndTag, true)) {

                        if (key == null) {
                            key = new FileLineWritable();
                        }

                        key.set(fileIn.getPos());

                        if (value == null) {
                            value = new Text();
                        }

                        value.set(buffer.getData(), 0, buffer.getLength());

                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;

    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            final int b = fileIn.read();
            // end of file:
            if (b == -1) {
                return false;
            }
            // save to buffer:
            if (withinBlock) {
                buffer.write(b);
            }

            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length) {
                    return true;
                }
            } else {
                i = 0;
            }
            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && fileIn.getPos() >= end) {
                return false;
            }
        }

    }
}
