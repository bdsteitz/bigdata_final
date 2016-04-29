 PDEs = LOAD 'hdfs:/user/steitzb/grad_students/prescription_events/*.csv' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 PDE = FOREACH PDEs GENERATE $0 as ID, (int)REGEX_EXTRACT($2, '([0-9]{6})', 1) as date, $3 as serviceId, $7 as cost;
 Benef = FOREACH Benefs GENERATE $0 as ID, (int)REGEX_EXTRACT($2, '([0-9]{6})', 1) as death;
 Death_Quarter = FOREACH Benef GENERATE ID, (death is null ? 0 : (death >= 200801 AND death <= 200806 ? 1 : (death >= 200807 AND death <= 200812 ? 2 : (death >= 200901 AND death <= 200906 ? 3 : (death >= 200907 AND death <= 200912 ? 4 : (death >= 201001 AND death
 <= 201006 ?  5 : 6)))))) as death_quarter, death;
 BiAnn = FOREACH PDE GENERATE ID, date, (date >= 200801 AND date <= 200806 ? 1 : (date >= 200807 AND date <= 200812 ? 2 :
 (date >= 200901 AND date <= 200906 ? 3 : (date >= 200907 AND date <= 200912 ? 4 : (date >= 201001 AND date <= 201006 ?
 5 : 6))))) as quarter, serviceId, cost;
 Filter_last_quarter = FILTER BiAnn BY quarter != 6;
 Grouped = GROUP Filter_last_quarter by (ID, quarter);
 Agg = FOREACH Grouped {
    uniqueSIDs = DISTINCT Filter_last_quarter.serviceId;
    GENERATE group.ID, group.quarter, COUNT(uniqueSIDs) as SIDct, SUM(Filter_last_quarter.cost) as quarterCost, 1.0 *
    SUM(Filter_last_quarter.cost) / 6 as
    monthlyCost;
}
Joined = JOIN Agg by ID LEFT OUTER, Death_Quarter BY ID;
Flat = FOREACH Joined GENERATE $0, $1, SIDct, quarterCost, monthlyCost, (death_quarter == 0 ? 0 :
($1 + 1 >= death_quarter ? 1 : 0)) as died;
Final = DISTINCT Flat;
DUMP Final;

