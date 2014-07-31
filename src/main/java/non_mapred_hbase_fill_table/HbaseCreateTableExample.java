package non_mapred_hbase_fill_table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseCreateTableExample {

    public static void main(String args[]) throws IOException {
        Configuration hc = HBaseConfiguration.create();
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("User"));
        ht.addFamily(new HColumnDescriptor("Id"));
        ht.addFamily(new HColumnDescriptor("Name"));

        System.out.println("connecting");
        HBaseAdmin hba = new HBaseAdmin(hc);

        System.out.println("Creating Table");
        hba.createTable(ht);

        System.out.println("Done......");
        hba.close();
    }

}
