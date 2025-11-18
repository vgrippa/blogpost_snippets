/* Current salary and title for a specific employee (ID 10005) */
SELECT e.first_name, e.last_name, s.salary, t.title
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no
JOIN titles t ON e.emp_no = t.emp_no
WHERE e.emp_no = 10005
  AND s.to_date = '9999-01-01'
  AND t.to_date = '9999-01-01';
