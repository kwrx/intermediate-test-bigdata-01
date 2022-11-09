

/* 1. Find the language with the highest average level */
SELECT name AS language, AVG(level) AS averageLevel
    FROM skills
GROUP BY language
ORDER BY averageLevel DESC
LIMIT 1;

/* 2. Find the province with the minimum number of employees */
SELECT livingPlace.province AS province, COUNT(*) AS numberOfEmployees
    FROM survey
GROUP BY province
ORDER BY numberOfEmployees ASC
LIMIT 1;

/* 3. For each language find the employee(s) with the highest level */
...


