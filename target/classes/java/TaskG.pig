/*
Identify "outdated" LinkBookPage. Return IDs and nicknames of persons that have not
accessed LinkBook for 90 days (i.e., no entries in the AccessLog in the last 90 days).

Map any activity in the last 90 days (accessTime < 129600) to have a list of all accounts that have been accessed in
the last 90 days. Mapped them with a “A” attached to make the joining easier. Next we map all linked book pages so
that there is a record of all of them. We then join the two mapper tasks in the reducer to  find which pages have and
have not had activity in the last 90 days, and from there we can output any linked book page that doesn’t have a join
in the activity
*/


accesslogs = LOAD 'input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);

activity = FILTER accessLogs by accesstime < 129600;

LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

j = JOIN LinkBookPages BY id LEFT OUTER, activity BY id;

page = FILTER LinkedBookPages by j::id is NULL;

output = FOREACH j GENERATE
            gage::id as id,  page::name as name;

STORE output INTO 'taskG.csv' USING PigStorage(',');
