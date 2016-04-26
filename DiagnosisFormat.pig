inpatientClaims = LOAD 'hdfs:/user/steitzb/grad_students/inpatient_claims/*.csv' USING
PigStorage(',')
AS(
PatientId,
ClaimId,
Segment,
ClaimStartDate,
ClaimEndDate,
InstitutionCcn,
PaymentAmount,
PaymentAmountNotMedicare,
AttedningPhysicianNpi,
OperatingPhysicianNpi,
OtherPhysicianNpi,
AdmissionDate,
AdmittingDiagnosis,
PassThruPerDiemAmount,
DeductibleAmount,
CoinsuranceLiabilityAmount,
BloodDeductibleLiabilityAmount,
UtilizationDayCount,
DischargeDate,
DiagnosisRelatedGroup,
Diagnosis1,
Diagnosis2,
Diagnosis3,
Diagnosis4,
Diagnosis5,
Diagnosis6,
Diagnosis7,
Diagnosis8,
Diagnosis9,
Diagnosis10,
Procedure1,
Procedure2,
Procedure3,
Procedure4,
Procedure5, 
Procedure6,
HCPCS1,
HCPCS2,
HCPCS3,
HCPCS4,
HCPCS5,
HCPCS6,
HCPCS7,
HCPCS8,
HCPCS9,
HCPCS10,
HCPCS11,
HCPCS12,
HCPCS13,
HCPCS14,
HCPCS15,
HCPCS16,
HCPCS17,
HCPCS18,
HCPCS19,
HCPCS20,
HCPCS21,
HCPCS22,
HCPCS23,
HCPCS24,
HCPCS25,
HCPCS26,
HCPCS27,
HCPCS28,
HCPCS29,
HCPCS30,
HCPCS31,
HCPCS32,
HCPCS33,
HCPCS34,
HCPCS35,
HCPCS36,
HCPCS37,
HCPCS38,
HCPCS39,
HCPCS40,
HCPCS41,
HCPCS42,
HCPCS43,
HCPCS44,
HCPCS45);

inpatientIcds = FOREACH inpatientClaims GENERATE ClaimId, PatientId, CONCAT(AdmissionDate, '000000') AS StartDateTime,
CONCAT(DischargeDate, '000000') AS EndDateTime,'ICD9' AS DataType,FLATTEN(TOBAG((chararray)AdmittingDiagnosis,
(chararray)Diagnosis1,
(chararray)Diagnosis2,
(chararray)Diagnosis3,                                                          (chararray)Diagnosis4,
(chararray)Diagnosis5,
(chararray)Diagnosis6,
(chararray)Diagnosis7,
(chararray)Diagnosis8,
(chararray)Diagnosis9,
(chararray)Diagnosis10,
(chararray)Procedure1,
(chararray)Procedure2,
(chararray)Procedure3,
(chararray)Procedure4,
(chararray)Procedure5,
(chararray)Procedure6))  AS DataValue;

filteredresultinter = FILTER inpatientIcds BY DataValue != '';

filteredresult = FOREACH filteredresultinter GENERATE ClaimId, PatientId, StartDateTime, EndDateTime, DataType,
SUBSTRING(DataValue,0,3);

beneficiaryClaims = LOAD 'hdfs:/user/steitzb/grad_students/beneficiary/*.csv' using PigStorage(',') as
(PatientId,
DateOfBirth,
DateOfDeath,
Sex,
RaceCode,
EndStgRenal,
State,
Country,
MonthsOfPartA,
MonthsOfPartB,
MonthsOfHmo,
MonthsPlanHmo,
ChronicConditionCode,
CHF,
CKD,
Cancer,
COPD,
Depression,
Diabetes,
HeartDisease,
Osteoporosis,
Arthritis,
Stroke,
InpatientAmount,
InpatientPatientAmount,
InpatientPayorAmount,
OutpatientAmount,
OutpatientBeneficiaryAmount,
OutpatientPayorAmount,
CarrierAmount,
CarrierPatientAmount,
CarrierPayorAmount);

beneficiaryDobs = FOREACH beneficiaryClaims GENERATE '00000000000000', PatientId, DateOfDeath, '00000000000000' AS
EndDateTime, 'DateOfDeath' AS DataType, (REGEX_EXTRACT(DateOfDeath, '([0-9]{6})', 1) is null ? 'Alive' : 'Dead') AS DataValue;

result = UNION filteredresult, beneficiaryDobs;

STORE result INTO 'hdfs:/user/goldstm/grad_students/patientcodes' USING PigStorage(',');
