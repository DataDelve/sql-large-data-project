SELECT DISTINCT
    case_month,
    res_state,
    count(res_state)
FROM
    covid_data
WHERE
    case_month = '2021-12'
GROUP BY
    res_state
ORDER BY
    res_state;
