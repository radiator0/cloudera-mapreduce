#!/bin/bash

FILE="all_performance_tests"

echo >> FILE
echo "Starting of tests..." >> FILE


echo "@@ First group @@" >> FILE

echo "Execution of rajeevw_5000.csv theman_5000.csv" >> $FILE
for i in {1..3}
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_5000.csv /testdata/theman_5000.csv
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of rajeevw_25000.csv theman_25000.csv" >> $FILE
for i in {1..3}
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_25000.csv /testdata/theman_25000.csv
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of rajeevw_50000.csv theman_50000.csv" >> $FILE
for i in {1..3}
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_50000.csv /testdata/theman_50000.csv
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

#TODO second group of tests

echo "Tests finished" >> $FILE