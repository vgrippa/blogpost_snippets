/* Average salary by department */
SELECT d.dept_name, ROUND(AVG(s.salary), 2) AS avg_salary
FROM salaries s
JOIN dept_emp de ON s.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE s.to_date = '9999-01-01' AND de.to_date = '9999-01-01'
GROUP BY d.dept_name
ORDER BY avg_salary DESC;
