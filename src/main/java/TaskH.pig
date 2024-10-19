/*
Identify people that have a relationship with someone (Associates); yet never accessed
their respective friend’s LinkBookPage. Report IDs and nicknames.

pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskH.pig

I think this one may just be wrong
*/

accessLogs = LOAD 'shared_folder/BigDataProject2/input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);
LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
associates = LOAD 'shared_folder/BigDataProject2/input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);

relationship1 = FOREACH associates GENERATE id1 AS id, id2 as associate;
relationship2 = FOREACH associates GENERATE id2 AS id, id1 as associate;
allAssociates = UNION relationship1,relationship2;

hasAccessedFriends = JOIN allAssociates BY (id, associate) LEFT OUTER, accessLogs BY (bywho,whatpage);

neverAccessedFriends = FILTER hasAccessedFriends by accessLogs::bywho is NULL;

distinctNeverAccessed = DISTINCT neverAccessedFriends;

neverAccessedPageInfo = JOIN distinctNeverAccessed BY associate, LinkBookPages BY id;

neverAccessedOutput = FOREACH neverAccessedPageInfo GENERATE
            LinkBookPages::id as id, LinkBookPages::name as nickname;
STORE neverAccessedOutput INTO 'shared_folder/BigDataProject2/output/taskH' USING PigStorage(',');
