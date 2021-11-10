dir=`pwd`
msdir=`cygpath -d "$dir"`
set -x

#winpty docker run -it --rm -p 9888:8888 -p 4040:4040 -v "$msdir:/home/jovyan/work" jupyter/pyspark-notebook bash
winpty docker run -it --rm -p 9888:8888 -p 4040:4040 -v "$msdir:/home/jovyan/work" jupyter/pyspark-notebook start-notebook.sh --NotebookApp.password='argon2:$argon2id$v=19$m=10240,t=10,p=8$3NRWj3MQGVsKT61YbQpZZA$2CDn8Sqh5zwxSNHT+Zg7Lw'
