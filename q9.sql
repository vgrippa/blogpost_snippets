/* Employees who received a salary above $120,000 */
SELECT DISTINCT e.emp_no, e.first_name, e.last_name, MAX(s.salary) AS highest_salary
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no
GROUP BY e.emp_no, e.first_name, e.last_name
HAVING highest_salary > 120000
ORDER BY highest_salary DESC
LIMIT 10;
