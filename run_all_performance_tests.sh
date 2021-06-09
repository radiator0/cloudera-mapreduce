#!/bin/bash

DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"` #add %3N as we want millisecond too
FILE="all_performance_tests$DATE_WITH_TIME"

echo >> FILE
echo "Starting of tests..." >> FILE

echo "@@ Prerun for prod data @@" >> FILE

echo "Execution test for prod data" >> $FILE
for i in {1..2}
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /ufc/rajeevw_ufcdata/2021-05-17 /ufc/theman90210_ufc-fight-dataset/2021-05-17.csv prod_data
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "@@ First group @@" >> FILE

echo "Execution of rajeevw_5000.csv theman_5000.csv" >> $FILE
for i in {1..2}
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_5000.csv /testdata/theman_5000.csv test_both_5000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

#echo "Execution of rajeevw_25000.csv theman_25000.csv" >> $FILE
#for i in {1..3}
#do
#  echo "# $i" >> $FILE
#  start=`date +%s`
#  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_25000.csv /testdata/theman_25000.csv
#  end=`date +%s`
#  runtime=$((end-start))
#  echo $runtime >> $FILE
#done

#echo "Execution of rajeevw_50000.csv theman_50000.csv" >> $FILE
#for i in {1..3}
#do
#  echo "# $i" >> $FILE
#  start=`date +%s`
#  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_50000.csv /testdata/theman_50000.csv
#  end=`date +%s`
#  runtime=$((end-start))
#  echo $runtime >> $FILE
#done

#TODO second group of tests

echo "Tests finished" >> $FILE