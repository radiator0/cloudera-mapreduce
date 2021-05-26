#!/bin/bash
PATH_TO_JAR="${1}"
INPUT_PATH1="${2}"
INPUT_PATH2="${3}"
DATE=$(date "+%Y-%m-%d")

/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage1Driver "${INPUT_PATH1}" "${INPUT_PATH2}" /ufc/${DATE}/stage1  #  Stage 1
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage2Driver /ufc/${DATE}/stage2-1 /ufc/${DATE}/stage2  #  Stage 2
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage3Driver /ufc/${DATE}/stage3-1 /ufc/${DATE}/stage3  #  Stage 3
/usr/bin/hadoop jar "${PATH_TO_JAR}" Stage4Driver /ufc/${DATE}/stage4-1 /ufc/${DATE}/stage4 /ufc/final  #  Stage 4
