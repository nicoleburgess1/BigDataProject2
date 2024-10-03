associates = LOAD 'input/Associates.csv'
                USING PigStorage(',')
                AS (colRel:int, id1:int, id2:int, date:int, description:chararray);
LinkBookPages = LOAD 'input/LinkBookPage.csv'
                USING PigStorage(',')
                AS (id:int, name:chararray, occupation:chararray, ncode:int, highestEdu:chararray);

associates = GROUP accessLogs by id1;
relationship1 = FOREACH associates GENERATE id1 AS associate;
relationship2 = FOREACH associates GENERATE id2 AS associate;

allAssociates = UNION relationship1,relationship2;
count = GROUP allAssociates by associate;
happiness = FOREACH count
            GENERATE group AS id, COUNT(allAssociates) AS numAssociations;

j = JOIN LinkBookPages BY id LEFT OUTER, happinessFactors BY id;
output = FOREACH j GENERATE
            LinkBookPages::name as name, (IsNull(happiness::numAssociations) ? 0 : happiness::numAssociations) AS happiness;

STORE output INTO 'taskD.csv' USING PigStorage(',');