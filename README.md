dm-xml-to-hbase-showcase
========================

* Map-Reduce job to load selected XML elements from HDFS into HBase table. (xmlToHBase)
  This is the main MR-jar that gets created when running Maven.
  Scripts to start are in `src/main/resources/xmlToHBase`

* Other Hadoop Map-Reduce and HBase examples, see the package infos inside the packages.


Eclipse Setup
-------------

For the MR jobs to run inside Eclipse you need

* add the `HADOOP_HOME` environment variable to the Environment tab of the launch configuration.
* `conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml"));`
