#!/bin/bash

# Install Java
brew install openjdk@17
JAVA_HOME=`brew info openjdk@17 | grep -A1 Installed | tail -1 | cut -d' ' -f1`

# Install Spark
cd ~/Downloads
if [ ! -f spark-4.0.0-bin-hadoop3.tgz ] ; then
    wget https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
fi
SPARK_HOME=$HOME/spark-4.0.0
if [ -d $SPARK_HOME ] ; then
    rm -rf $SPARK_HOME 
fi
mkdir -p $SPARK_HOME
cd $SPARK_HOME
tar xvzf ~/Downloads/spark-4.0.0-bin-hadoop3.tgz
#pip install pyspark[pandas_on_spark,sql,connect]

# Create bootstrap file
runspark=$HOME/bin/run-spark
if [ -f $runspark ] ; then 
   rm -f $runspark
fi
cat >$runspark <<!
export JAVA_HOME="$JAVA_HOME"
export SPARK_HOME="$SPARK_HOME"
export PATH="\$JAVA_HOME/bin:\$SPARK_HOME/bin:\$PATH"
spark-shell
!
chmod +x $runspark
