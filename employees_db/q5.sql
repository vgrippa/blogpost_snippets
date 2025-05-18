/* Employees currently working in the “Sales” department */
SELECT e.emp_no, e.first_name, e.last_name, d.dept_name
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE d.dept_name = 'Sales'
  AND de.to_date = '9999-01-01'
LIMIT 10;
