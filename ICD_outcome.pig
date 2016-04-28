 OutptICDs = LOAD 'hdfs:/user/steitzb/outpt_icd/*' using PigStorage(',');
 Benefs = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',');
 OutptICD = FOREACH OutptICDs GENERATE $0 as ID, $1 as date, $2 as ICD1, $3 as ICD2, $4 as ICD3, $5 as ICD4, $6 as ICD5, $7 as ICD6, $8 as ICD7, $9 as ICD8, $10 as ICD9, $11 as ICD10, $12 as outcome;
 Benef = FOREACH Benefs GENERATE $0 as ID, REGEX_EXTRACT($1, '([0-9]{6})', 1) as birth;
 Joined = JOIN OutptICD by ID LEFT OUTER, Benef BY ID;
 Reduced = DISTINCT Joined;
 Final = FOREACH Reduced GENERATE OutptICD::ID, date, (date is null ? 999 : (YearsBetween(ToDate(date, 'yyyyMM'), ToDate(birth, 'yyyyMM')))) as
 age, ICD1, ICD2, ICD3, ICD4, ICD5, ICD6, ICD7, ICD8, ICD9, ICD10, outcome;
 DUMP Final;
