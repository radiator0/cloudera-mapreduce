#!/bin/bash
hadoop fs -mkdir /testdata
hadoop fs -touchz /testdata/emptyfile
hadoop fs -put test-data/rajeevw_5000.csv /testdata
hadoop fs -put test-data/rajeevw_25000.csv /testdata
hadoop fs -put test-data/rajeevw_50000.csv /testdata
hadoop fs -put test-data/theman_5000.csv /testdata
hadoop fs -put test-data/theman_25000.csv /testdata
hadoop fs -put test-data/theman_50000.csv /testdata