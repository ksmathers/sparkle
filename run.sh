LAUNCHER=winpty
LOCALPORT=9888
MONITORUI=9999
LOCALDIR=`pwd`/jupyter
DOCKER=docker

dir=`pwd`
set -x

$DOCKER run \
        -it \
	--name sparkle \
        --rm \
        -p $LOCALPORT:8888 \
        -p $MONITORUI:4040 \
        -v "/$LOCALDIR:/home/jovyan/work" \
        -v "/$HOME/.aws:/home/jovyan/.aws" \
        sparkle-notebook start-notebook.sh --NotebookApp.password='argon2:$argon2id$v=19$m=10240,t=10,p=8$3NRWj3MQGVsKT61YbQpZZA$2CDn8Sqh5zwxSNHT+Zg7Lw'
