/* All department managers' birth dates */
SELECT DISTINCT e.emp_no, e.first_name, e.last_name, e.birth_date
FROM employees e
JOIN dept_manager dm ON e.emp_no = dm.emp_no
ORDER BY e.birth_date DESC;
