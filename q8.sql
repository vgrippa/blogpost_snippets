/* Employees with more than one title historically */
SELECT e.emp_no, e.first_name, e.last_name, COUNT(t.title) AS total_titles
FROM employees e
JOIN titles t ON e.emp_no = t.emp_no
GROUP BY e.emp_no, e.first_name, e.last_name
HAVING total_titles > 1
ORDER BY total_titles DESC
LIMIT 10;
