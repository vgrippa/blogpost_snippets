/* Employees hired after January 1, 1995 */
SELECT emp_no, first_name, last_name, hire_date
FROM employees
WHERE hire_date > '1995-01-01'
ORDER BY hire_date ASC
LIMIT 10;
