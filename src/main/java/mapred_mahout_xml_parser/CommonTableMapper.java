package mapred_mahout_xml_parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonTableMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log log = LogFactory.getLog(CommonTableMapper.class);

    @Override
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String readLine = value.toString();
        System.out.println("'" + readLine + "'");
        try {
            XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(
                    new ByteArrayInputStream(readLine.getBytes()));
            try {

                String propertyRowID = "";
                String propertyStatus = "";
                String currentElement = "";
                while (reader.hasNext()) {
                    int code = reader.next();
                    switch (code) {
                    case 2: // '\002'
                    case 3: // '\003'
                    default:
                        break;

                    case 1: // '\001'
                        currentElement = reader.getLocalName();
                        log.info("currentElement: " + currentElement);
                        break;

                    case 4: // '\004'
                        if (currentElement.equalsIgnoreCase("name")) {
                            propertyRowID = propertyRowID + reader.getText();
                            break;
                        }
                        if (currentElement.equalsIgnoreCase("value"))
                            propertyStatus = propertyStatus + reader.getText();
                        break;
                    }
                }
                context.write(new Text(propertyRowID.trim()), new Text(propertyStatus.trim()));

                log.info("job done");
                System.out.println("job done");

            } finally {
                reader.close();
            }
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }
}
