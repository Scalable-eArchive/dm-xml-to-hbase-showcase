package smallfiles;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CFMapper extends Mapper<FileLineWritable, Text, Text, Text> {

    private HTable ht;

    private static final String HBASE_DEFAULT_TABLE_NAME = "xmlToHbase";

    private static final int HBASE_DEFAULT_BUFFER_SIZE = 1024 * 1024 * 12;

    // private final Text keyText = new Text();
    // private final Text valueText = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        ht = new HTable(context.getConfiguration(), HBASE_DEFAULT_TABLE_NAME);
        ht.setAutoFlush(true);
        ht.setWriteBufferSize(HBASE_DEFAULT_BUFFER_SIZE);
    }

    /**
     * The mapper parses xml, and puts valuable data to Hbase table.
     */
    @Override
    public void map(FileLineWritable key, Text val, Context context) throws IOException, InterruptedException {
        String currentElement = "";

        // This attribute will be the row in the Hbase table 
        String fileName = "";

        // column family: general
        String md5checksum = "";
        String lastmodified = "";
        String size = "";

        // column family: format
        String identity = "";
        String wellformed = "";
        String valid = "";
        String charset = "";
        String linebreak = "";

        // column family: image
        String compressionScheme = "";
        String imageWidth = "";
        String imageHeight = "";
        try {
            // <---------------------- begin XML parsing --------------------------->
            final XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(
                    new ByteArrayInputStream(val.toString().getBytes()));

            while (reader.hasNext()) {
                final int code = reader.next();
                switch (code) {
                case START_ELEMENT:
                    currentElement = reader.getLocalName();
                    break;
                case CHARACTERS:
                    // <----------------- fileinfo --------------------->
                    if (currentElement.equalsIgnoreCase("filename")) {
                        fileName += reader.getText();
                    } else if (currentElement.equalsIgnoreCase("md5checksum")) {
                        md5checksum += reader.getText();
                    } else if (currentElement.equalsIgnoreCase("size")) {
                        size += reader.getText();
                    } else if (currentElement.equalsIgnoreCase("fslastmodified")) {
                        lastmodified += reader.getText();
                    } else if (currentElement.equalsIgnoreCase("identity")) {
                        identity += identity;

                        // <----------------- filestatus --------------------->
                    } else if (currentElement.equals("well-formed")) {
                        wellformed += reader.getText();
                    } else if (currentElement.equals("valid")) {
                        valid += reader.getText();
                    } else if (currentElement.equals("charset")) {
                        charset += reader.getText();
                    } else if (currentElement.equals("linebreak")) {
                        linebreak += reader.getText();
                    } else if (currentElement.equals("compressionScheme")) {
                        compressionScheme += reader.getText();
                    } else if (currentElement.equals("imageWidth")) {
                        imageWidth += reader.getText();
                    } else if (currentElement.equals("imageHeight")) {
                        imageHeight += reader.getText();
                    }

                    break;
                default:
                    // ignore
                    break;
                }
            }
            reader.close();

            // <---------------------- end of XML parsing --------------------------->

            // keyText.set(fileName.trim());
            // valueText.set(md5checksum.trim());
            // context.write(keyText, valueText);

            final Put inHbase = new Put(Bytes.toBytes(fileName.trim()));

            inHbase.add(Bytes.toBytes("general"), Bytes.toBytes("md5checksum"), Bytes.toBytes(md5checksum));
            inHbase.add(Bytes.toBytes("general"), Bytes.toBytes("size"), Bytes.toBytes(size));
            inHbase.add(Bytes.toBytes("general"), Bytes.toBytes("lastmodified"), Bytes.toBytes(lastmodified));

            inHbase.add(Bytes.toBytes("format"), Bytes.toBytes("identity"), Bytes.toBytes(identity));
            inHbase.add(Bytes.toBytes("format"), Bytes.toBytes("wellformed"), Bytes.toBytes(wellformed));
            inHbase.add(Bytes.toBytes("format"), Bytes.toBytes("valid"), Bytes.toBytes(valid));
            inHbase.add(Bytes.toBytes("format"), Bytes.toBytes("charset"), Bytes.toBytes(charset));
            inHbase.add(Bytes.toBytes("format"), Bytes.toBytes("linebreak"), Bytes.toBytes(linebreak));

            inHbase.add(Bytes.toBytes("image"), Bytes.toBytes("compressionscheme"), Bytes.toBytes(compressionScheme));
            inHbase.add(Bytes.toBytes("image"), Bytes.toBytes("imagewidth"), Bytes.toBytes(imageWidth));
            inHbase.add(Bytes.toBytes("image"), Bytes.toBytes("imageHeight"), Bytes.toBytes(imageHeight));

            ht.put(inHbase);

            // <--------------------------- job done -------------------------------->

            // System.out.println(String.format("stats :   filename : %s,  md5checksum: %s, size: %s, lastmodified: %s, wellformed: %s, valid: %s, charset: %s, linebreak: %s, compressionScheme: %s, imageWidth: %s, imageHeight: %s, ", 
            // fileName, md5checksum, size, lastmodified, wellformed, valid, charset, linebreak, compressionScheme, imageWidth, imageHeight ));

        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException {
        ht.flushCommits();
        ht.close();
    }

}
