/* Current salary for a specific employee */
SELECT e.emp_no, e.first_name, e.last_name, s.salary
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no
WHERE e.emp_no = 10001 AND s.to_date = '9999-01-01';
