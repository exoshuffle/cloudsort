#!/bin/bash

set -ex

BIN_DIR=cloudsort/bin

prepare() {
    rm -rf $BIN_DIR
    mkdir -p $BIN_DIR
}

install_gensort() {
    DIR=$BIN_DIR/gensort
    mkdir -p $DIR
    pushd $DIR
    TARFILE=gensort-linux-1.5.tar.gz
    wget http://www.ordinal.com/try.cgi/$TARFILE
    tar xf $TARFILE
    popd
}

install_grafana() {
    DIR=$BIN_DIR/grafana
    APP=grafana-9.2.2
    mkdir -p $DIR
    pushd $DIR
    TARFILE=$APP.linux-amd64.tar.gz
    wget https://dl.grafana.com/oss/release/$TARFILE
    tar xf $TARFILE --strip-components=1
    popd
}

_install_github_binary() {
    PROJ=$1
    BIN=$2
    VER=$3
    SEP=$4
    DIR=$BIN_DIR
    mkdir -p $DIR
    pushd $DIR
    TARNAME=${BIN}-${VER}${SEP}linux-amd64
    TARFILE=$TARNAME.tar.gz
    wget https://github.com/$PROJ/$BIN/releases/download/v$VER/$TARFILE
    tar xf $TARFILE
    mv $TARNAME $BIN
    popd
}

install_prometheus() {
    _install_github_binary prometheus prometheus 2.39.1 .
}

install_node_exporter() {
    _install_github_binary prometheus node_exporter 1.4.0 .
}

install_jaeger() {
    _install_github_binary jaegertracing jaeger 1.31.0 -
}

cleanup() {
    find . -type f -name '*.tar.gz' -delete
}

show_files() {
    find $BIN_DIR -type d
}

show_help() {
    set +x
    echo "Usage: $0 [binary1] [binary2]..."
    echo "  where [binary#] can be {gensort,prometheus,node_exporter,jaeger}."
    echo "  If no argument is supplied, will install all binaries."
    set -x
}

args="$@"
if [ -z "$args" ]; then
    args="gensort prometheus node_exporter grafana"
fi

prepare
for arg in $args
do
    "install_$arg"
done
cleanup
show_files
