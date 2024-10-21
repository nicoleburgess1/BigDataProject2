/*
Report all owners of a LinkBookPage who are more popular than an average user,
namely, those who have more relationships than the average number of relationships
across all owners LinkBookPages.

pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskF.pig

idk how to do the average
*/

associates = LOAD 'shared_folder/BigDataProject2/input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);

relationship1 = FOREACH associates GENERATE id1 AS id, id2 as associate;
relationship2 = FOREACH associates GENERATE id2 AS id, id1 as associate;
allAssociates = UNION relationship1,relationship2;

groupedAssociates = GROUP allAssociates BY id;
popularity = FOREACH groupedAssociates
            GENERATE group AS id, COUNT(allAssociates) AS numAssociations;

allAssociations = FOREACH (GROUP popularity BY 'dummy') GENERATE
            SUM(popularity.numAssociations) AS associations;

allUsers = FOREACH (GROUP popularity BY 'dummy') GENERATE
            COUNT(popularity) AS totalUsers;

associationsAndUsers = CROSS allAssociations, allUsers;
avg = FOREACH associationsAndUsers
            GENERATE (double)associations / totalUsers AS average;

aboveAverage = FILTER popularity BY numAssociations > avg.average;

LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

j = JOIN LinkBookPages BY id, aboveAverage BY id;
out = FOREACH j GENERATE LinkBookPages::id AS id, LinkBookPages::name AS name, aboveAverage::numAssociations AS associations;
STORE out INTO 'shared_folder/BigDataProject2/output/TaskF' USING PigStorage(',');