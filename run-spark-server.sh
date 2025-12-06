#!bash
#pip install grpcio grpcio-status pyarrow
set -x
docker run -it --rm --name myspark -p 7077:7077 -p 8081:8080\
  -v `pwd`/jupyter/testdata:/opt/spark/data/testdata \
  -e SPARK_MASTER_IP=0.0.0.0 \
  apache/spark bash 
#//opt/spark/sbin/start-all.sh
#  -e SPARK_NO_DAEMONIZE=true \
