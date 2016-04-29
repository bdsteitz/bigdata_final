 Outpt_ICDs = LOAD 'hdfs:/user/parrsk/Outpt_ICD_6mo/*' using PigStorage(',');
 ICD = FOREACH Outpt_ICDs GENERATE $0 as ID, $1 as time, $2..$22, $23 as death;
 Provs = LOAD 'hdfs:/user/parrsk/Provider_6mo/*' using PigStorage(',');
 Prov = FOREACH Provs GENERATE $0 as ID, $1 as time, $2;
 PDEs = LOAD 'hdfs:/user/parrsk/PDE_6mo/*' using PigStorage(',');
 PDE = FOREACH PDEs GENERATE $0 as ID, $1 as time, $2..$4;
 Join1 = JOIN ICD BY (ID, time) LEFT OUTER, Prov BY (ID, time);
 Join2 = JOIN Join1 BY (ICD::ID, ICD::time) LEFT OUTER, PDE BY (ID, time);
 Flat = FOREACH Join2 GENERATE $0..$22, $26, $29..$31, $23 as death;
 Reduced = DISTINCT Flat;
 DUMP Reduced;
