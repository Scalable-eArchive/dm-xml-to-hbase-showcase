INFILE="input-fits-files.txt"
IN=$1;
IN_TMP_DIR=`echo $IN | rev`;
IN_DIR=`echo "${IN_TMP_DIR#*/}" | rev`;
IN_DIR2=$IN_DIR;
HDFS_LOCATION="hdfs:///testdata"
HDFS_LOCATION_INPUT="hdfs:///testdata/${IN_DIR2##*/}"
HDFS_LOCATION_OUTPUT="$HDFS_LOCATION_INPUT-output"
HADOOP="/home/funnyboy/hadoop-1.2.1/bin/hadoop"
TOOLSPECS="$HDFS_LOCATION/toolspecs"

if !([ -f "$1" ])
then
  echo "WARNING: Second parameter is not a file: $1";
  echo "List of file should be specified.";
  echo "Example: ./fits-input-gen-copy.sh ~/work/filesraw/*";
  exit;
fi

echo -n > $INFILE

#call script with param *
while [ $# -gt 0 ]; do
 echo "fits apply --input=\"$HDFS_LOCATION_INPUT/${1##*/}\" --output=\"$HDFS_LOCATION_OUTPUT/fits.out.${1##*/}.xml\"" >> $INFILE 
 shift
done

#create our own input directory on HDFS
if ( `$HADOOP fs -test -d $HDFS_LOCATION_INPUT`)
then
  echo "We clean up ...  $HDFS_LOCATION_INPUT";
  $HADOOP fs -rmr $HDFS_LOCATION_INPUT;
fi
$HADOOP fs -mkdir $HDFS_LOCATION_INPUT;
echo "HDFS input dir created ...  $HDFS_LOCATION_INPUT";


#copy the input dir to HDFS
$HADOOP fs -put $IN_DIR/* $HDFS_LOCATION_INPUT/;
echo "Copy files from $IN_DIR  --->  $HDFS_LOCATION_INPUT is completed";


#create our own output directory on HDFS
if ( `$HADOOP fs -test -d $HDFS_LOCATION_OUTPUT`)
then
  echo "We clean up ...  $HDFS_LOCATION_OUTPUT";
  $HADOOP fs -rmr $HDFS_LOCATION_OUTPUT;
fi
$HADOOP fs -mkdir $HDFS_LOCATION_OUTPUT;
echo "HDFS output dir created ...  $HDFS_LOCATION_OUTPUT";


#copy toolspecs to HDFS
if !( `$HADOOP fs -test -d $TOOLSPECS`)
then
  echo "Toolspecs is being copied to $TOOLSPECS"
  $HADOOP fs -put toolspecs/* $TOOLSPECS;
fi
