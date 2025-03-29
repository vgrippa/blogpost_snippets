/* Retrieve the employee count per department by gender (currently active) */
SELECT d.dept_name, e.gender, COUNT(*) AS num_employees
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE de.to_date = '9999-01-01'
GROUP BY d.dept_name, e.gender
ORDER BY d.dept_name, e.gender;
