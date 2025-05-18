/* Query to Find Employees Who Have Never Been Assigned to a Department: */
SELECT e.emp_no, e.first_name, e.last_name
FROM employees e
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
WHERE de.emp_no IS NULL;
