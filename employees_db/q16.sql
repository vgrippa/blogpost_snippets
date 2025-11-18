/* List all employees hired in 1990 */
SELECT emp_no, first_name, last_name, hire_date
FROM employees
WHERE hire_date >= '1990-01-01' AND hire_date <= '1990-12-31'
ORDER BY hire_date ASC;
