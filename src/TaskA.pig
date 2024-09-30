raw = LOAD 'excite.log' USING PigStorage('\t') AS (user, id, time, query);
clean1 = FILTER raw BY id > 20 AND id < 100;
clean2 = FOREACH clean1 GENERATE
user, time,
org.apache.pig.tutorial.sanitze(query) as query;
user_groups = GROUP clean2 BY (user, query);
user_query_counts = FOREACH user_groups
GENERATE group, COUNT(clean2), MIN(clean2.time), MAX(clean2.time);
STORE user_query_counts INTO 'uq_counts.csv' USING PigStorage(',');