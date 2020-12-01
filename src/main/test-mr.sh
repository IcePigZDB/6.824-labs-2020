#!/bin/sh
set -x
#
# basic map-reduce test
#
# Does not consider a worker do a parttime job ,and overwrite the job.
RACE=
wc_test=1
# i add a test that map and reduce func will delay for a while.
# this code can not pass wc-wait_test
# TODO how to fix runtime too small which lead to 
# the sisuation is that
# set MaxTaskRunTime small,many task will (p=1/2)wait for a while ,
# we will make a lot of duplica task.when return a task to worker,i 
# has check the status of task ,if TaskStatusFinish the task will be removed,
# it shorter the time to do map phase.After map phase ,there may be some worker 
# wait in m.taskCh,when reduce phase init,after task put into taskCh,it disapper
# mysteriously.
# to fix it ,set wc_wait_test to 1 and open Debug flag in ../mrapps/common.go
wc_wait_test=0
indexer_test=1
map_parallelism_test=1
reduce_parallelism_test=1
crash_test=1
# uncomment this to run the tests with the Go race detector.
#RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin wc-wait.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrmaster.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

# first word-count
# word-count-wait
# space matters
if [ $wc_test -eq 1 ]
then
# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
# ../mrsequential ../../mrapps/wc.so ../pg-my.txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s ../mrmaster ../pg*txt &
# timeout -k 2s 180s ../mrmaster ../pg-my.txt &

# give the master time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait

# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait ; wait ; wait
fi

# word-count-wait
# space matters
if [ $wc_wait_test -eq 1 ]
then
# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
# ../mrsequential ../../mrapps/wc.so ../pg-my.txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 20s ../mrmaster ../pg*txt &
# timeout -k 2s 180s ../mrmaster ../pg-my.txt &

# give the master time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 20s ../mrworker ../../mrapps/wc-wait.so &
timeout -k 2s 20s ../mrworker ../../mrapps/wc-wait.so &
timeout -k 2s 20s ../mrworker ../../mrapps/wc-wait.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait

# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc_wait test: PASS
else
  echo '---' wc_wait  output is not the same as mr-correct-wc.txt
  echo '---' wc_wait test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait ; wait ; wait
fi

# now indexer
if [ $indexer_test -eq 1 ]
then
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait ; wait
fi

if [ $map_parallelism_test -eq 1 ]
then
echo '***' Starting map parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait ; wait
fi

if [ $reduce_parallelism_test -eq 1 ]
then
echo '***' Starting reduce parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait ; wait
fi

# generate the correct output
if [ $crash_test -eq 1 ]
then
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*



echo '***' Starting crash test.

rm -f mr-done
(timeout -k 2s 180s ../mrmaster ../pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's masterSock()
SOCKNAME=/var/tmp/824-mr-`id -u`
# start mrworker until all map reduce work done
( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait
wait
wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi
fi

if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
