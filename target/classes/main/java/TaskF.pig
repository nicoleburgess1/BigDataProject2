/*
Report all owners of a LinkBookPage who are more popular than an average user,
namely, those who have more relationships than the average number of relationships
across all owners LinkBookPages.

pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskF.pig

idk how to do the average
*/

associates = LOAD 'input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);
popularity = FOREACH associates
            GENERATE group AS id1, COUNT(id2) AS numAssociations;
average = average(numAssociations);

aboveAverage = FILTER popularity BY numAssociations > average;

LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

j = JOIN LinkBookPages BY id LEFT OUTER, aboveAverage BY id;

output = FOREACH j GENERATE
            LinkBookPages::id as id,  LinkBookPages::name as name, numAssociations as associations;

STORE output INTO 'taskF.csv' USING PigStorage(',');
