/* Retrieve first 10 employee records */
SELECT emp_no, first_name, last_name, hire_date
FROM employees
WHERE hire_date > DATE_SUB('1995-01-01', INTERVAL FLOOR(RAND() * 3650) DAY)
ORDER BY hire_date ASC
LIMIT 10;
