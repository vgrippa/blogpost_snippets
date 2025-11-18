/* Employees with a title change on a specific date */
SELECT e.emp_no, e.first_name, e.last_name, t.title
FROM employees e
JOIN titles t ON e.emp_no = t.emp_no
WHERE t.from_date = '1995-03-22'
ORDER BY e.emp_no;
