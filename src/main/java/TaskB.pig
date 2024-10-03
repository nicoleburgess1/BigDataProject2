accesslogs = LOAD 'input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);
accesses = GROUP accessLogs by whatpage;
count = FOREACH accesses
    GENERATE group AS whatpage, COUNT(LinkBookPages) AS numAccesses;

LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
pageInfo = FOREACH LinkBookPages generate id, name, occupation;
C = JOIN count by whatPage, LinkBookPAge by id;

STORE C INTO 'taskB.csv' USING PigStorage(',');