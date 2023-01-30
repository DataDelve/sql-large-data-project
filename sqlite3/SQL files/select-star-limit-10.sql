SELECT id,
       case_month,
       res_state,
       state_fips_code,
       res_county,
       county_fips_code,
       age_group,
       sex,
       race,
       ethnicity,
       case_positive_specimen_interval,
       case_onset_interval,
       process,
       exposure_yn,
       current_status,
       symptom_status,
       hosp_yn,
       icu_yn,
       death_yn,
       underlying_conditions_yn
  FROM covid_data
  LIMIT 10;