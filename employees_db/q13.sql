/* Query to Retrieve Departments with No Managers Assigned Currently */
SELECT d.dept_name
FROM departments d
LEFT JOIN dept_manager dm ON d.dept_no = dm.dept_no AND dm.to_date = '9999-01-01'
WHERE dm.emp_no IS NULL;
