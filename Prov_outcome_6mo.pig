 Provs = LOAD 'hdfs:/user/steitzb/grad_students/prov_list/*' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 Prov = FOREACH Provs GENERATE $0 as ID, (int)$1 as date, $2 as provCt;
 Benef = FOREACH Benefs GENERATE $0 as ID, (int)REGEX_EXTRACT($2, '([0-9]{6})', 1) as death;
 Death_Half = FOREACH Benef GENERATE ID, (death is null ? 0 : (death >= 200801 AND death <= 200806 ? 1 : (death >=
 200807 AND death <= 200812 ? 2 : (death >= 200901 AND death <= 200906 ? 3 : (death >= 200907 AND death <= 200912 ? 4 : (death >= 201001 AND death <= 201006 ?  5 : 6)))))) as death_half, death;
 BiAnn = FOREACH Prov GENERATE ID, date, (date >= 200801 AND date <= 200806 ? 1 : (date >= 200807 AND date <= 200812 ? 2 :
 (date >= 200901 AND date <= 200906 ? 3 : (date >= 200907 AND date <= 200912 ? 4 : (date >= 201001 AND date <= 201006 ?
 5 : 6))))) as half, provCt;
 FLG = FILTER BiAnn BY half != 6;
 Grouped = GROUP FLG by (ID, half);
 Agg = FOREACH Grouped GENERATE group.ID, group.half, SUM(FLG.provCt) as provCt;
Joined = JOIN Agg by ID LEFT OUTER, Death_Half BY ID;
Flat = FOREACH Joined GENERATE $0, $1, provCt, (death_half == 0 ? 0 : ($1 + 1 >= death_half ? 1 : 0)) as died;
Reduced = DISTINCT Flat;
DUMP Reduced;

