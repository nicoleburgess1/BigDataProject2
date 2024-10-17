/*
pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskA.pig
Works!
*/

LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
edu = GROUP LinkBookPages by highestEdu;
count = FOREACH edu
GENERATE group AS highestEdu, COUNT(LinkBookPages) AS eduCount;
STORE count INTO 'shared_folder/BigDataProject2/output/TaskA' USING PigStorage(',');