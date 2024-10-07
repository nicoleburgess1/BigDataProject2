/*
Report all owners of a LinkBookPage who are more popular than an average user,
namely, those who have more relationships than the average number of relationships
across all owners LinkBookPages.
*/

associates = LOAD 'input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);
associates = GROUP accessLogs by id1;


LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);


