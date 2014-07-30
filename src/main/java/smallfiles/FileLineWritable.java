package smallfiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.primitives.Longs;

public class FileLineWritable implements WritableComparable<FileLineWritable> {

    public long offset;
    public String fileName;

    @Override
    public void readFields(DataInput in) throws IOException {
        offset = in.readLong();
        fileName = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    @Override
    public int compareTo(FileLineWritable that) {
        final int cmp = fileName.compareTo(that.fileName);
        if (cmp != 0) {
            return cmp;
        }
        return (int) Math.signum((double) (offset - that.offset));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (fileName == null ? 0 : fileName.hashCode());
        result = prime * result + (int) (offset ^ offset >>> 32);
        return result;
    }

    @Override
    public boolean equals(Object obj) { 
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FileLineWritable other = (FileLineWritable) obj;
        if (fileName == null) {
            if (other.fileName != null) {
                return false;
            }
        } else if (!fileName.equals(other.fileName)) {
            return false;
        }
        if (offset != other.offset) {
            return false;
        }
        return true;
    }

    public void set(long pos) {
        offset = pos;
    }

}
