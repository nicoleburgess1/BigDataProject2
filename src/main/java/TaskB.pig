accessLogs = LOAD 'shared_folder/BigDataProject2/input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);

LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

accesses = GROUP accessLogs by whatpage;
count = FOREACH accesses
    GENERATE group AS whatpage, COUNT(accessLogs) AS numAccesses;


pageInfo = FOREACH LinkBookPages GENERATE id as whatpage, name, occupation;
C = JOIN count by whatpage, pageInfo by whatpage;



STORE C INTO 'shared_folder/BigDataProject2/output/taskB' USING PigStorage(',');