SELECT DISTINCT
    case_month,
    res_state,
    count(res_state) AS state_total
FROM
    covid_data
GROUP BY
    case_month, res_state
ORDER BY
    case_month, res_state;