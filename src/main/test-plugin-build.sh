#!bin/bash
# use for fast build debug version run in vscode.
set -x
RACE= 
# debug build need extra para
# go build -gcflags='all=-N -l'
# make sure software is freshly built.
# debugplug 
cd .. # src/main
rm *.so
(go build -o wc-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/wc.go) || exit 1
(go build -o indexer-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/indexer.go) || exit 1
(go build -o mtiming-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/mtiming.go) || exit 1
(go build -o rtiming-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/rtiming.go) || exit 1
(go build -o crash-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/crash.go) || exit 1
(go build -o nocrash-build.so -gcflags='all=-N -l' $RACE -buildmode=plugin ../mrapps/nocrash.go) || exit 1
# normal plug
(cd ../mrapps && rm *.so && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 
# exe
rm mrmaster mrworker mrsequential
(go build $RACE mrmaster.go) || exit 1
(go build $RACE mrworker.go) || exit 1
(go build $RACE mrsequential.go) || exit 1