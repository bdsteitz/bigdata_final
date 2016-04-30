 PDEs = LOAD 'hdfs:/user/steitzb/grad_students/prescription_events/*.csv' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 PDE = FOREACH PDEs GENERATE $0 as ID, REGEX_EXTRACT($2, '([0-9]{6})', 1) as date, $3 as serviceId, $5 as daySupply, $7 as cost;
 DayCost = FOREACH PDE GENERATE *, (1.0 * cost / daySupply) as costPerDay;
 Grouped = GROUP DayCost by (ID, date);
 Agg = FOREACH Grouped {
    uniqueSIDs = DISTINCT DayCost.serviceId;
    GENERATE group, COUNT(uniqueSIDs) as SIDct, SUM(DayCost.cost) as moCost, SUM(DayCost.costPerDay) as dailyCost;
 };
 Benef = FOREACH Benefs GENERATE $0 as ID, REGEX_EXTRACT($2, '([0-9]{6})', 1) as death;
 Joined = JOIN Agg by group.ID LEFT OUTER, Benef by ID;
 Reduced = DISTINCT Joined;
 Flagged = FOREACH Reduced Generate group.ID, group.date, SIDct, moCost, dailyCost, (death is null ? 0 : ((int)(MonthsBetween(ToDate(death, 'yyyyMM'), ToDate(group.date,'yyyyMM'))) <= 6 ? 1 : 0)) as Flag;
 DUMP Flagged;
