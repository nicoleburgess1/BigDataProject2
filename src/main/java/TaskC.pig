LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
edu = GROUP LinkBookPages by highestEdu;
count = FOREACH edu
GENERATE group AS highestEdu, COUNT(LinkBookPages) AS eduCount;
bachelors = FILTER count by highestEdu == 'BachelorsDegree';
STORE bachelors INTO 'shared_folder/BigDataProject2/output/taskC' USING PigStorage(',');
