/* Query to Identify Employees with Salaries that Have Decreased Over Time */
SELECT e.emp_no, e.first_name, e.last_name, s1.salary AS initial_salary, s2.salary AS recent_salary
FROM employees e
JOIN salaries s1 ON e.emp_no = s1.emp_no
JOIN salaries s2 ON e.emp_no = s2.emp_no
WHERE s1.to_date < s2.from_date
  AND s1.salary > s2.salary
  AND s2.to_date = '9999-01-01';
