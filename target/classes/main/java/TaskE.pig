/*Determine which people have favorites. That is, for each LinkBookPage owner,
  determine how many total accesses to LinkBookPage they have made (as reported in the
  AccessLog) and how many distinct LinkBookPage they have accessed in total. As for the
  identifier of each LinkBookPage owner, you don’t have to report name. IDs are enough
  Output: ID, totalNumAccesses, numDistinct
  */

accesslogs = LOAD 'input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);
accesses = GROUP accessLogs by bywho;
totalCount = FOREACH accesses GENERATE group AS ID, count(accessLogs) AS numAccesses;

distinctAccesses = DISTINCT(FOREACH accessLogs GENERATE bywho AS ownerID, whatpage);
groupedByID = GROUP distinctAccessses BY ownerID;
distinctCount = FOREACH distinctAccesses GENERATE bywho AS ID, count(groupedByID) AS numDistinct;

j = JOIN totalCount BY ID LEFT OUTER, distinctCount BY ID;
output = FOREACH j GENERATE
            totalCount::ID as ID, totalCOunt:: numAccesses as numAccesses, distinctCount:: numDistinct as numDistinct;

STORE output INTO 'taskE.csv' USING PigStorage(',');