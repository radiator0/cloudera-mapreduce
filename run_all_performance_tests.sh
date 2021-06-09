#!/bin/bash

DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"` #add %3N as we want millisecond too
FILE="all_performance_tests$DATE_WITH_TIME"
COUNT=2

echo >> FILE
echo "Starting of tests..." >> FILE

echo "@@ First group - both files @@" >> FILE

echo "Execution of test with rajeevw_5000.csv and theman_5000.csv" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_5000.csv /testdata/theman_5000.csv test_both_5000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_500000.csv and theman_500000.csv" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_500000.csv /testdata/theman_500000.csv test_both_500000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_1000000.csv and theman_1000000.csv" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_1000000.csv /testdata/theman_1000000.csv test_both_1000000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done


echo "@@ Second group - only one file @@" >> FILE

echo "Execution of test with rajeevw_5000.csv and EMPTY" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_5000.csv /testdata/emptyfile test_OF_5000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_500000.csv and EMPTY" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_500000.csv /testdata/emptyfile test_OF_500000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_1000000.csv and EMPTY" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_1000000.csv /testdata/emptyfile test_OF_1000000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Tests finished" >> $FILE