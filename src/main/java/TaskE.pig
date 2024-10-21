/*Determine which people have favorites. That is, for each LinkBookPage owner,
  determine how many total accesses to LinkBookPage they have made (as reported in the
  AccessLog) and how many distinct LinkBookPage they have accessed in total. As for the
  identifier of each LinkBookPage owner, you don’t have to report name. IDs are enough
  Output: ID, totalNumAccesses, numDistinct
  pig -x local /home/ds503/shared_folder/BigDataProject2/src/main/java/TaskE.pig

  Doesn't do anything currently, I may have messed it up worse than we had it trying to get it to work
  */

accessLogs = LOAD 'shared_folder/BigDataProject2/input/accessLogs.csv'
                USING PigStorage(',')
                AS (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);
totalAccesses = GROUP accessLogs BY bywho;
totalCount = FOREACH totalAccesses
    GENERATE group AS ID, COUNT(accessLogs) AS numTotalAccesses;


distinctAccesses = DISTINCT (FOREACH accessLogs GENERATE bywho AS ownerID, whatpage);
groupedByID = GROUP distinctAccesses BY ownerID;
distinctCount = FOREACH groupedByID GENERATE group AS ID, COUNT(distinctAccesses) AS numDistinct;

j = JOIN totalCount BY ID LEFT OUTER, distinctCount BY ID;
accessOutput = FOREACH j GENERATE
            totalCount::ID as ID, totalCount::numTotalAccesses as numAccesses, distinctCount::numDistinct as numDistinct;

STORE accessOutput INTO 'shared_folder/BigDataProject2/output/taskE' USING PigStorage(',');