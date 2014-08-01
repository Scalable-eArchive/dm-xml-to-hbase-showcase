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
  (maybe only needed if not in global environment)

* `conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml"));`

* for Windows the Hadoop binaries for Hadoop 2.2 x64 have been added to the root of the project, so the
  examples run out of Eclipse. This is propably connected to non-global environment.
  
  