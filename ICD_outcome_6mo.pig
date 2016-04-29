 ICDs = LOAD 'hdfs:/user/steitzb/grad_students/group_icd/outpt_icd_group.txt' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 ICD = FOREACH ICDs GENERATE $0 as ID, (int)$1 as date, $2 as age, $3 as ICDC1, $4 as ICDC2, $5 as ICDC3, $6 as ICDC4,$7
 as ICDC5, $8 as ICDC6, $9 as ICDC7, $10 as ICDC8, $11 as ICDC9, $12 as ICDC10, $13 as ICDC11, $14 as ICDC12, $15 as
 ICDC13, $16 as ICDC14, $17 as ICDC15, $18 as ICDC16, $19 as ICDC17, $20 as ICDC18, $21 as ICDC19, $22 as
 total_ICD_count;
 Benef = FOREACH Benefs GENERATE $0 as ID, (int)REGEX_EXTRACT($2, '([0-9]{6})', 1) as death;
 Death_HalfYr = FOREACH Benef GENERATE ID, (death is null ? 0 : (death >= 200801 AND death <= 200806 ? 1 : (death >=
 200807 AND death <= 200812 ? 2 : (death >= 200901 AND death <= 200906 ? 3 : (death >= 200907 AND death <= 200912 ? 4 :
 (death >= 201001 AND death <= 201006 ?  5 : 6)))))) as death_half_yr, death;
 BiAnn = FOREACH ICD GENERATE ID, date, age, (date >= 200801 AND date <= 200806 ? 1 : (date >= 200807 AND date <= 200812
 ? 2 : (date >= 200901 AND date <= 200906 ? 3 : (date >= 200907 AND date <= 200912 ? 4 : (date >= 201001 AND date <=
 201006 ? 5 : 6))))) as half_yr, ICDC1, ICDC2, ICDC3, ICDC4, ICDC5, ICDC6, ICDC7, ICDC8, ICDC9, ICDC10, ICDC11, ICDC12, ICDC13, ICDC14, ICDC15, ICDC16, ICDC17, ICDC18, ICDC19, total_ICD_count;
 FLG = FILTER BiAnn BY half_yr != 6;
 Grouped = GROUP FLG by (ID, half_yr);
 Agg = FOREACH Grouped GENERATE group.ID, group.half_yr, MAX(FLG.age), SUM(FLG.ICDC1), SUM(FLG.ICDC2), SUM(FLG.ICDC3),
 SUM(FLG.ICDC4), SUM(FLG.ICDC5), SUM(FLG.ICDC6), SUM(FLG.ICDC7), SUM(FLG.ICDC8), SUM(FLG.ICDC9), SUM(FLG.ICDC10),
 SUM(FLG.ICDC11), SUM(FLG.ICDC12), SUM(FLG.ICDC13), SUM(FLG.ICDC14), SUM(FLG.ICDC15), SUM(FLG.ICDC16), SUM(FLG.ICDC17),
 SUM(FLG.ICDC18), SUM(FLG.ICDC19), SUM(FLG.total_ICD_count);
Joined = JOIN Agg by ID LEFT OUTER, Death_HalfYr BY ID;
Flat = FOREACH Joined GENERATE $0..$22, (death_half_yr == 0 ? 0 : ($1 + 1 >= death_half_yr ? 1 : 0)) as died;
Final = DISTINCT Flat;
DUMP Final;

