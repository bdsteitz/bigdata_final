 PDEs = LOAD 'hdfs:/user/steitzb/grad_students/prescription_events/*.csv' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 PDE = FOREACH PDEs GENERATE $0 as ID, REGEX_EXTRACT($2, '([0-9]{6})', 1) as date, $3 as serviceId, $8 as cost;
 Grouped = GROUP PDE by (ID, date);
 Agg = FOREACH Grouped {
    uniqueSIDs = DISTINCT PDE.serviceId;
    GENERATE group, COUNT(uniqueSIDs) as SIDct, SUM(PDE.cost) as moCost;
 };
 Benef = FOREACH Benefs GENERATE $0 as ID, REGEX_EXTRACT($2, '([0-9]{6})', 1) as death;
 Joined = JOIN Agg by group.ID LEFT OUTER, Benef by ID;
 Flagged = FOREACH Joined Generate group.ID, group.date, SIDct, moCost, (death is null ? 0 : ((int)(MonthsBetween(ToDate(death,
 'yyyyMM'), ToDate(group.date,'yyyyMM'))) <= 6 ? 1 : 0)) as Flag;
 DUMP Flagged;
