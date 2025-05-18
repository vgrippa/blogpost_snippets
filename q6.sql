/* Employees who have been managers */
SELECT DISTINCT e.emp_no, e.first_name, e.last_name
FROM employees e
JOIN dept_manager dm ON e.emp_no = dm.emp_no;
