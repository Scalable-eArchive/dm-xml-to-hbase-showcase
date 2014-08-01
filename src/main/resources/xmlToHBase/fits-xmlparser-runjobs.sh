
INFILE="input-fits-files.txt"
HADOOP="/home/funnyboy/hadoop-1.2.1/bin/hadoop"

TOMAR="toolwrapper.jar"
TOOLSPECS="/testdata/toolspecs"
#number of lines processed my one Mapper
NLINE=50

exec 6< $INFILE;
read INLINE1 <&6; 
exec 6<&-;
echo $INLINE1;
INLINE1_TMP=`echo $INLINE1 | rev`;
INLINE1_TMP2=`echo "${INLINE1_TMP#*/}" | rev`;
INXML=${INLINE1_TMP2##*--output=\"};
OUTXML=$INXML-output;
XMLPARSER="xmlToHBase.jar"

#run ToMaR
$HADOOP jar $TOMAR -i $INFILE -r $TOOLSPECS -n $NLINE


#run xmlToHBase
$HADOOP jar $XMLPARSER $INXML/* $OUTXML;
