#!bash
stages=${*:-1 2 3 4 5 6 7 8 9}
# build.sh is to build docker image from within MacBook
#. ~/bin/arad-de
DOCKER_TAG=sparkle-notebook

mkdist() {
    srcdir=$1
    echo " - $srcdir"
    app=`basename $srcdir`
    #rm -rf dist/$app
    mkdir -p dist/$app
    pip wheel -q $srcdir -w dist/$app
    rm -f dist/$app/*macos*
}

if grep -q 1 <<< "$stages" ; then
    echo "Stage 1: Rebuild dependencies"
    mkdist ~/git/sparkle
    cp -r ~/etc dist/etc
fi

if grep -q 2 <<< "$stages" ; then
    echo "Stage 2: Build docker image $DOCKER_TAG"
    docker build . -t $DOCKER_TAG
fi

if grep -q run <<< "$stages" ; then
    echo "Stage 3: Run docker image $DOCKER_TAG"

    cwd=`pwd`
    LOCALPORT=9888
    MONITORUI=9999
    LOCALDIR=$cwd/jupyter
    SRCDIR=$cwd
    DOTAWS=$HOME/.aws
    DOCKER=docker
    LAUNCHER=
    if [ "$MSYSTEM" = "MINGW64" ] ; then
	LOCALDIR=`cygpath -w $LOCALDIR`
	SRCDIR=`cygpath -w $SRCDIR`
	DOTAWS=`cygpath -w $DOTAWS`
	LAUNCHER=winpty
    fi

    set -x

    $LAUNCHER $DOCKER run \
        -it \
        --rm \
        -p $LOCALPORT:8888 \
        -p $MONITORUI:4040 \
        -v "$SRCDIR:/home/jovyan/src" \
        -v "$LOCALDIR:/home/jovyan/work" \
        -v "$DOTAWS:/home/jovyan/.aws" \
        sparkle-notebook start-notebook.sh --NotebookApp.password='argon2:$argon2id$v=19$m=10240,t=10,p=8$3NRWj3MQGVsKT61YbQpZZA$2CDn8Sqh5zwxSNHT+Zg7Lw'
fi
