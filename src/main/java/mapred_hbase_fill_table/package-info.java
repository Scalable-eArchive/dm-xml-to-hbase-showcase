/**
 * Fill HBase as Map-Reduce Target. The TableMapper is not part of HBase client but in the main HBase (hbase-server) jar.
 * Does a word count on a HBase table as MR job.
 * @see http://bigdataprocessing.wordpress.com/2012/07/27/hadoop-hbase-mapreduce-examples/
 */
package mapred_hbase_fill_table;

