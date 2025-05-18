/* Query to Retrieve the Most Recent Hire in Each Department */
SELECT d.dept_name, e.emp_no, e.first_name, e.last_name, e.hire_date
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE de.to_date = '9999-01-01'
  AND e.hire_date = (
      SELECT MAX(e2.hire_date)
      FROM employees e2
      JOIN dept_emp de2 ON e2.emp_no = de2.emp_no
      WHERE de2.dept_no = de.dept_no
        AND de2.to_date = '9999-01-01'
  );
