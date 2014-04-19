rm -rf output
export HADOOP_CLASSPATH=my.jar
HADOOP_OPTS='-Dmapred.compress.map.output=false' hadoop jar my.jar dyc.maxtemperature.MaxTemperature 1901 output
hadoop dyc.maxtemperature.MaxTemperatureDriver -conf conf/hadoop-local.xml data/1901 output
