#!/bin/bash
PATH_TO_JAR="${1}"
INPUT_PATH1="${2}"
INPUT_PATH2="${3}"
DATE=$(date "+%Y-%m-%d")
DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"` #add %3N as we want millisecond too

set -e

/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage1Driver "${INPUT_PATH1}" "${INPUT_PATH2}" /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage1  #  Stage 1
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage2Driver /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage1 /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage2  #  Stage 2
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage3Driver /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage2 /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage3  #  Stage 3
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage4Driver /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage3 /tests/${DATE_WITH_TIME}/ufc/${DATE}/stage4 /tests/${DATE_WITH_TIME}/final  #  Stage 4
