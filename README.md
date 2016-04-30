# bigdata_final

Files:

format_final_dataset: joins data from outpatient, provider, pdes into final dataset for full model

format_icd_age.pig: Format ICD codes for future ICD catalog ordering

format_icd_outcome_grouped.pig: Aggregate ICD chapter counts per patient during 6mo period and obtain outcome status.
Same script works for inpatient and outpatient ICD data. Requires output from process_icd_catalog.py

format_inpatient_icd.pig: Format inpatient claims for further ICD-related transformations (not used in final project)

format_patient_icds.py: Format inpatient and outpatient claims data for use in ICD catalog classifications 

get_inpatient_diagnosis.pig: Get inpatient diagnoses for inpatient data predictions (not used in final project)

get_patient_ages.py: Get patient age based on beneficiary file

group_6mo_pde.pig: Group pde data into 6-month intervals, calculated aggregate pde measures, ascertain outcome status

machine_learning.py: Machine learning script for all data

process_icd_catalog.py: Count ICD codes per chapter per patient claim

process_provs.py: get number of providers per patient per claim
