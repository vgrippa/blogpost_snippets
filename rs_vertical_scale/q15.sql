/* Query to Calculate the Percentage of Employees by Gender in Each Department: */
SELECT d.dept_name, e.gender,
       ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY d.dept_name), 2) AS gender_percentage
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE de.to_date = '9999-01-01'
GROUP BY d.dept_name, e.gender;
