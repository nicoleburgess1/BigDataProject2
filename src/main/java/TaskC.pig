LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
edu = GROUP LinkBookPages by highestEdu;
count = FOREACH edu
GENERATE group AS highestEdu, COUNT(LinkBookPages) AS eduCount;
bachelors = FILTER count by edu == 'bachelorsDegree';
STORE count INTO 'taskC.csv' USING PigStorage(',');