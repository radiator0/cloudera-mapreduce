#!/bin/bash

DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"` #add %3N as we want millisecond too
FILE="all_performance_tests$DATE_WITH_TIME"
COUNT=1

echo >> FILE
echo "Starting of tests..." >> FILE

#echo "@@ Prerun for prod data @@" >> FILE

#echo "Execution of test for prod data" >> $FILE
#for i in {1..2}
#do
#  echo "# $i" >> $FILE
#  start=`date +%s`
#  bash execute-test.sh /home/cloudera/Untitled.jar /ufc/rajeevw_ufcdata/2021-05-17 /ufc/theman90210_ufc-fight-dataset/2021-05-17.csv prod_data
#  end=`date +%s`
#  runtime=$((end-start))
#  echo $runtime >> $FILE
#done

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

echo "Execution of test with rajeevw_25000.csv and theman_25000.csv" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_25000.csv /testdata/theman_25000.csv test_both_25000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_50000.csv and theman_50000.csv" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_50000.csv /testdata/theman_50000.csv test_both_50000
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

echo "Execution of test with rajeevw_25000.csv and EMPTY" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_25000.csv /testdata/emptyfile test_OF_25000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Execution of test with rajeevw_50000.csv and EMPTY" >> $FILE
for (( i=1; i<=$COUNT; i++ ))
do
  echo "# $i" >> $FILE
  start=`date +%s`
  bash execute-test.sh /home/cloudera/Untitled.jar /testdata/rajeevw_50000.csv /testdata/emptyfile test_OF_50000
  end=`date +%s`
  runtime=$((end-start))
  echo $runtime >> $FILE
done

echo "Tests finished" >> $FILE