/* All active department heads in the 'Finance' department */
SELECT e.emp_no, e.first_name, e.last_name, d.dept_name
FROM employees e
JOIN dept_manager dm ON e.emp_no = dm.emp_no
JOIN departments d ON dm.dept_no = d.dept_no
WHERE d.dept_name = 'Finance'
  AND dm.to_date = '9999-01-01'
ORDER BY e.emp_no;
