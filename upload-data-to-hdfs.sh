#!/bin/bash
hadoop fs -rm -r -f /testdata
hadoop fs -mkdir /testdata
hadoop fs -touchz /testdata/emptyfile
hadoop fs -put /home/cloudera/testdata/rajeevw_5000.csv /testdata
hadoop fs -put /home/cloudera/testdata/rajeevw_500000.csv /testdata
hadoop fs -put /home/cloudera/testdata/rajeevw_1000000.csv /testdata
hadoop fs -put /home/cloudera/testdata/theman_5000.csv /testdata
hadoop fs -put /home/cloudera/testdata/theman_500000.csv /testdata
hadoop fs -put /home/cloudera/testdata/theman_1000000.csv /testdata