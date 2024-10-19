/*
For each LinkBookPage, compute the “happiness factor” of its owner. That is, for each
LinkBookPage, report the owner’s nickname, and the number of relationships they have.
For page owners that aren't listed in Associates, return a score of zero. Please note that
we maintain a symmetric relationship, take that into account in your calculations.
pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskD.pig
Works!
*/

associates = LOAD 'shared_folder/BigDataProject2/input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);
LinkBookPages = LOAD 'shared_folder/BigDataProject2/input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

relationship1 = FOREACH associates GENERATE id1 AS id;
relationship2 = FOREACH associates GENERATE id2 AS id;

allAssociates = UNION relationship1,relationship2;
count = GROUP allAssociates by id;
happiness = FOREACH count
            GENERATE group AS id, COUNT(allAssociates) AS numAssociations;


j = JOIN LinkBookPages BY id LEFT OUTER, happiness BY id;

happinessCount = FOREACH j GENERATE
            LinkBookPages::name as name,
            (happiness::numAssociations IS NULL ? 0 : happiness::numAssociations) AS happiness;

STORE happinessCount INTO 'shared_folder/BigDataProject2/output/taskD' USING PigStorage(',');