export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export HADOOP_HOME=%(hadoop_home)s
export HADOOP_CONF_DIR=$HADOOP_HOME/conf
export HADOOP_LOG_DIR=$HADOOP_HOME/logs
export HADOOP_SLAVES=$HADOOP_CONF_DIR/slaves
#export HADOOP_PID_DIR=%(datadir)s/pids
#export HADOOP_TMP_DIR=%(datadir)s/tmp
#export TMP=/hadoop/$USER/tmp
# The maximum amount of heap to use, in MB. Default is 1000.
export HADOOP_HEAPSIZE=%(hadoop_heapsize)s
export HADOOP_NAMENODE_OPTS="-XX:+UseParallelGC -Dcom.sun.management.jmxremote"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote"
export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH
