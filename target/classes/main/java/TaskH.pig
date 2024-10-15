/*
Identify people that have a relationship with someone (Associates); yet never accessed
their respective friend’s LinkBookPage. Report IDs and nicknames.
*/

accesslogs = LOAD 'input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);
LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);
associates = LOAD 'input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);

associates = GROUP accessLogs by id1;
relationship1 = FOREACH associates GENERATE id1 AS id, id2 as associate;
relationship2 = FOREACH associates GENERATE id2 AS id, id1 as associate;
allAssociates = UNION relationship1,relationship2;

hasAccessedFriends = JOIN allAssociates BY (id, associate) LEFT OUTER, accessLogs BY (bywho,whatpage);

neverAccessedFriends = FILTER hasAcceddedFriends by accessLogs::bywho is NULL;

neverAccessedPageInfo = JOIN neverAccessedFriends BY id LEFT OUTER, LinkBookPages BY id;

output = FOREACH neverAccessedPageInfo GENERATE
            LinkBookPages::id as id, name as nickname;
STORE output INTO 'taskH.csv' USING PigStorage(',');
