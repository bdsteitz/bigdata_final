# bigdata_final

Files:

format_icd_age.pig: Format ICD codes for future ICD catalog ordering

format_icd_outcome_grouped.pig: Aggregate ICD chapter counts per patient during 6mo period and obtain outcome status

format_inpatient_icd.pig: Format inpatient claims for further ICD-related transformations

format_patient_icds.py: Format inpatient and outpatient claims data for ICD catalog classifications 

get_inpatient_diagnosis.pig: Get inpatient diagnoses for inpatient data predictions

get_patient_ages.py: Get patient age based on beneficiary file

group_6mo_pde.pig: Group pde data into 6-month intervals, calculated aggregate pde measures, ascertain outcome status

machine_learning.py: Machine learning script for all data

process_icd_catalog.py: Count ICD codes per chapter per patient claim

process_provs.py: get number of providers per patient per claim
