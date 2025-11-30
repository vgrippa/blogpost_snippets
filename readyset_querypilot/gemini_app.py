import sys
import time
import random
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from mysql.connector import Error, pooling

# ==========================================
# 1. SYSTEM CONFIGURATION
# ==========================================
DB_CONFIG = {
    'host': 'XXXX',
    'port': 3306,
    'user': 'readyset_repl',
    'password': 'readyset_repl',
    'database': 'employees',
    'connection_timeout': 5
}

# Real-world Constants
ACTIVE_DATE = '9999-01-01'  # The 'magic date' for active records in this schema
VALID_DEPTS = ['d001', 'd002', 'd003', 'd004', 'd005', 'd006', 'd007', 'd008', 'd009']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
TITLES = ['Senior Engineer', 'Staff', 'Engineer', 'Senior Staff', 'Assistant Engineer', 'Technique Leader', 'Manager']

# ==========================================
# 2. DATA GENERATORS
# ==========================================
def get_emp_id():
    """Generates an ID in the 'sweet spot' of the dataset."""
    return random.randint(10001, 200000)

def get_recent_date_range():
    """Simulates looking at data from the 90s (when this DB is set)."""
    year = random.randint(1990, 1998)
    return f"{year}-01-01", f"{year}-12-31"

# ==========================================
# 3. REAL-WORLD SCENARIOS (30 Queries)
# ==========================================

# --- MODULE: AUTHENTICATION & SESSION ---

def q01_user_login(cursor):
    """Scenario: User enters username/password. System fetches basic identity."""
    eid = get_emp_id()
    cursor.execute("SELECT emp_no, first_name, last_name, gender, hire_date FROM employees WHERE emp_no = %s", (eid,))
    cursor.fetchall()
    return "Auth: Login"

def q02_session_validation(cursor):
    """Scenario: Checking if the user is still employed (Active check)."""
    eid = get_emp_id()
    # Join dept_emp to see if they have an active department assignment
    query = "SELECT de.dept_no FROM dept_emp de WHERE de.emp_no = %s AND de.to_date = %s"
    cursor.execute(query, (eid, ACTIVE_DATE))
    cursor.fetchall()
    return "Auth: Validate Active"

# --- MODULE: DASHBOARD WIDGETS ---

def q03_widget_team_birthdays(cursor):
    """Scenario: 'Upcoming Birthdays' widget on the dashboard."""
    # Finding people born in a specific month (e.g., current month)
    m = random.randint(1, 12)
    query = "SELECT first_name, birth_date FROM employees WHERE month(birth_date) = %s LIMIT 5"
    cursor.execute(query, (m,))
    cursor.fetchall()
    return "Widget: Birthdays"

def q04_widget_new_hires(cursor):
    """Scenario: 'Welcome New Joinees' widget."""
    start, end = get_recent_date_range()
    query = "SELECT first_name, last_name, hire_date FROM employees WHERE hire_date BETWEEN %s AND %s ORDER BY hire_date DESC LIMIT 5"
    cursor.execute(query, (start, end))
    cursor.fetchall()
    return "Widget: New Hires"

def q05_widget_my_dept_manager(cursor):
    """Scenario: Dashboard showing 'Your Manager' contact info."""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT e.first_name, e.last_name 
        FROM dept_manager dm 
        JOIN employees e ON dm.emp_no = e.emp_no 
        WHERE dm.dept_no = %s AND dm.to_date = %s
    """
    cursor.execute(query, (dept, ACTIVE_DATE))
    cursor.fetchall()
    return "Widget: My Manager"

# --- MODULE: EMPLOYEE PROFILE (360 View) ---

def q06_profile_header(cursor):
    """Scenario: Loading the top header of a profile page."""
    eid = get_emp_id()
    cursor.execute("SELECT first_name, last_name, birth_date, gender FROM employees WHERE emp_no = %s", (eid,))
    cursor.fetchall()
    return "Profile: Header"

def q07_profile_current_job(cursor):
    """Scenario: 'Job' tab - displaying current Title and Dept."""
    eid = get_emp_id()
    query = """
        SELECT t.title, d.dept_name 
        FROM titles t 
        JOIN dept_emp de ON t.emp_no = de.emp_no 
        JOIN departments d ON de.dept_no = d.dept_no
        WHERE t.emp_no = %s AND t.to_date = %s AND de.to_date = %s
    """
    cursor.execute(query, (eid, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Profile: Current Job"

def q08_profile_compensation(cursor):
    """Scenario: 'Compensation' tab - showing current base pay."""
    eid = get_emp_id()
    cursor.execute("SELECT salary, from_date FROM salaries WHERE emp_no = %s AND to_date = %s", (eid, ACTIVE_DATE))
    cursor.fetchall()
    return "Profile: Current Pay"

def q09_profile_timeline_titles(cursor):
    """Scenario: 'History' tab - List of all previous titles."""
    eid = get_emp_id()
    cursor.execute("SELECT title, from_date, to_date FROM titles WHERE emp_no = %s ORDER BY from_date DESC", (eid,))
    cursor.fetchall()
    return "Profile: Title Hist"

def q10_profile_timeline_transfers(cursor):
    """Scenario: 'History' tab - List of department transfers."""
    eid = get_emp_id()
    query = """
        SELECT d.dept_name, de.from_date, de.to_date 
        FROM dept_emp de 
        JOIN departments d ON de.dept_no = d.dept_no 
        WHERE de.emp_no = %s ORDER BY de.from_date DESC
    """
    cursor.execute(query, (eid,))
    cursor.fetchall()
    return "Profile: Dept Hist"

# --- MODULE: PAYROLL & COMPENSATION ---

def q11_payroll_run_batch(cursor):
    """Scenario: Batch job fetching all active salaries for a department."""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT s.emp_no, s.salary 
        FROM salaries s 
        JOIN dept_emp de ON s.emp_no = de.emp_no 
        WHERE de.dept_no = %s AND s.to_date = %s AND de.to_date = %s
        LIMIT 50
    """
    cursor.execute(query, (dept, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Payroll: Dept Batch"

def q12_verify_pay_slip(cursor):
    """Scenario: Employee clicking 'View PDF' on a specific pay slip."""
    eid = get_emp_id()
    # Looking for a specific past salary record
    cursor.execute("SELECT salary, from_date, to_date FROM salaries WHERE emp_no = %s LIMIT 1", (eid,))
    cursor.fetchall()
    return "Payroll: View Slip"

def q13_comp_review_audit(cursor):
    """Scenario: HR Manager finding people who haven't had a raise recently."""
    # "Recently" simulated by checking older start dates on current salary
    cursor.execute("SELECT emp_no, salary, from_date FROM salaries WHERE to_date = %s AND from_date < '1998-01-01' LIMIT 10", (ACTIVE_DATE,))
    cursor.fetchall()
    return "Payroll: Stale Pay"

def q14_high_earner_audit(cursor):
    """Scenario: Compliance report for high-bracket employees."""
    cursor.execute("SELECT emp_no, salary FROM salaries WHERE salary > 120000 AND to_date = %s LIMIT 10", (ACTIVE_DATE,))
    cursor.fetchall()
    return "Payroll: High Earners"

def q15_salary_equity_check(cursor):
    """Scenario: Checking salary for a specific job title to ensure fairness."""
    title = random.choice(TITLES)
    query = """
        SELECT s.salary 
        FROM salaries s 
        JOIN titles t ON s.emp_no = t.emp_no 
        WHERE t.title = %s AND s.to_date = %s AND t.to_date = %s 
        LIMIT 20
    """
    cursor.execute(query, (title, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Payroll: Equity Check"

# --- MODULE: TALENT ACQUISITION & SEARCH ---

def q16_autocomplete_lastname(cursor):
    """Scenario: User typing in the global search bar."""
    # 3 chars is standard trigger for autocomplete
    prefix = random.choice(LAST_NAMES)[:3] + "%"
    cursor.execute("SELECT emp_no, first_name, last_name FROM employees WHERE last_name LIKE %s LIMIT 8", (prefix,))
    cursor.fetchall()
    return "Search: Autocomplete"

def q17_directory_search_exact(cursor):
    """Scenario: Looking up a specific colleague by name."""
    lname = random.choice(LAST_NAMES)
    cursor.execute("SELECT emp_no, first_name, last_name, hire_date FROM employees WHERE last_name = %s LIMIT 20", (lname,))
    cursor.fetchall()
    return "Search: Exact Name"

def q18_find_peers(cursor):
    """Scenario: 'My Team' view - finding others in same dept."""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT e.first_name, e.last_name, t.title 
        FROM dept_emp de
        JOIN employees e ON de.emp_no = e.emp_no
        JOIN titles t ON e.emp_no = t.emp_no
        WHERE de.dept_no = %s AND de.to_date = %s AND t.to_date = %s
        LIMIT 10
    """
    cursor.execute(query, (dept, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Search: Peer List"

def q19_onboarding_list(cursor):
    """Scenario: Generating list of users needing onboarding (Hired recently)."""
    start, end = get_recent_date_range()
    cursor.execute("SELECT emp_no, first_name, hire_date FROM employees WHERE hire_date >= %s LIMIT 10", (start,))
    cursor.fetchall()
    return "Talent: Onboarding"

def q20_veteran_awards(cursor):
    """Scenario: Finding employees eligible for '10 Year Service Award'."""
    # Hired before 1990 and still active
    query = """
        SELECT e.emp_no, e.first_name, e.hire_date 
        FROM employees e 
        JOIN dept_emp de ON e.emp_no = de.emp_no 
        WHERE e.hire_date < '1989-01-01' AND de.to_date = %s 
        LIMIT 10
    """
    cursor.execute(query, (ACTIVE_DATE,))
    cursor.fetchall()
    return "Talent: Veterans"

# --- MODULE: ORG CHART & MANAGEMENT ---

def q21_org_chart_down(cursor):
    """Scenario: Manager viewing their direct reports."""
    dept = random.choice(VALID_DEPTS)
    # Simplified: Get all active staff in a manager's dept
    query = """
        SELECT e.first_name, e.last_name, t.title 
        FROM dept_emp de 
        JOIN employees e ON de.emp_no = e.emp_no 
        JOIN titles t ON de.emp_no = t.emp_no
        WHERE de.dept_no = %s AND de.to_date = %s AND t.to_date = %s
        LIMIT 15
    """
    cursor.execute(query, (dept, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Org: Direct Reports"

def q22_manager_list_global(cursor):
    """Scenario: Executive overview of all current department heads."""
    query = """
        SELECT d.dept_name, e.first_name, e.last_name 
        FROM dept_manager dm 
        JOIN departments d ON dm.dept_no = d.dept_no 
        JOIN employees e ON dm.emp_no = e.emp_no 
        WHERE dm.to_date = %s
    """
    cursor.execute(query, (ACTIVE_DATE,))
    cursor.fetchall()
    return "Org: All Managers"

def q23_open_req_check(cursor):
    """Scenario: Counting current headcount vs budget (Simulated)."""
    dept = random.choice(VALID_DEPTS)
    # Just counting active rows to simulate headcount check (Simulated with fetchall len)
    cursor.execute("SELECT emp_no FROM dept_emp WHERE dept_no = %s AND to_date = %s LIMIT 100", (dept, ACTIVE_DATE))
    cursor.fetchall()
    return "Org: Headcount"

def q24_title_roster(cursor):
    """Scenario: Finding all Senior Engineers for a technical meeting."""
    title = "Senior Engineer"
    query = """
        SELECT e.first_name, e.last_name 
        FROM titles t 
        JOIN employees e ON t.emp_no = e.emp_no 
        WHERE t.title = %s AND t.to_date = %s 
        LIMIT 10
    """
    cursor.execute(query, (title, ACTIVE_DATE))
    cursor.fetchall()
    return "Org: Title Roster"

# --- MODULE: DIVERSITY & INCLUSION (Reporting) ---

def q25_diversity_report_gender(cursor):
    """Scenario: Fetching data for gender distribution charts."""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT e.gender, e.emp_no 
        FROM dept_emp de 
        JOIN employees e ON de.emp_no = e.emp_no 
        WHERE de.dept_no = %s AND de.to_date = %s 
        LIMIT 50
    """
    cursor.execute(query, (dept, ACTIVE_DATE))
    cursor.fetchall()
    return "D&I: Gender Sample"

def q26_retention_risk_check(cursor):
    """Scenario: Identify people stuck in same role for too long (Flight risk)."""
    # Title start date < 1994 and still active
    query = """
        SELECT emp_no, title, from_date 
        FROM titles 
        WHERE from_date < '1994-01-01' AND to_date = %s 
        LIMIT 10
    """
    cursor.execute(query, (ACTIVE_DATE,))
    cursor.fetchall()
    return "D&I: Flight Risk"

# --- MODULE: SYSTEM ADMIN ---

def q27_admin_user_audit(cursor):
    """Scenario: Admin viewing raw user record for debugging."""
    eid = get_emp_id()
    cursor.execute("SELECT * FROM employees WHERE emp_no = %s", (eid,))
    cursor.fetchall()
    return "Admin: Raw Dump"

def q28_admin_orphan_check(cursor):
    """Scenario: Checking for users with no department (Data integrity)."""
    # This usually returns empty in a good DB, but the query is realistic
    cursor.execute("SELECT emp_no FROM employees WHERE emp_no NOT IN (SELECT emp_no FROM dept_emp) LIMIT 10")
    cursor.fetchall()
    return "Admin: Integrity"

def q29_export_dept_roster_csv(cursor):
    """Scenario: User clicking 'Export to CSV'."""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT e.emp_no, e.first_name, e.last_name, t.title, s.salary
        FROM employees e
        JOIN dept_emp de ON e.emp_no = de.emp_no
        JOIN titles t ON e.emp_no = t.emp_no
        JOIN salaries s ON e.emp_no = s.emp_no
        WHERE de.dept_no = %s 
          AND de.to_date = %s AND t.to_date = %s AND s.to_date = %s
        LIMIT 25
    """
    cursor.execute(query, (dept, ACTIVE_DATE, ACTIVE_DATE, ACTIVE_DATE))
    cursor.fetchall()
    return "Admin: Export CSV"

def q30_system_health(cursor):
    """Scenario: Health check / Ping."""
    cursor.execute("SELECT 1")
    cursor.fetchall()
    return "Sys: Heartbeat"


# ==========================================
# 4. DISPATCHER & POOL
# ==========================================
QUERY_POOL = [
    q01_user_login, q02_session_validation, q03_widget_team_birthdays, q04_widget_new_hires, q05_widget_my_dept_manager,
    q06_profile_header, q07_profile_current_job, q08_profile_compensation, q09_profile_timeline_titles, q10_profile_timeline_transfers,
    q11_payroll_run_batch, q12_verify_pay_slip, q13_comp_review_audit, q14_high_earner_audit, q15_salary_equity_check,
    q16_autocomplete_lastname, q17_directory_search_exact, q18_find_peers, q19_onboarding_list, q20_veteran_awards,
    q21_org_chart_down, q22_manager_list_global, q23_open_req_check, q24_title_roster, q25_diversity_report_gender,
    q26_retention_risk_check, q27_admin_user_audit, q28_admin_orphan_check, q29_export_dept_roster_csv, q30_system_health
]

def execute_request(connection_pool):
    connection = None
    cursor = None
    query_name = "Unknown"
    
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            query_func = random.choice(QUERY_POOL)
            query_name = query_func(cursor)
            return (True, query_name, None)
            
    except Error as e:
        return (False, query_name, str(e))
    except Exception as e:
        return (False, query_name, str(e))
        
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close() 

def main():
    if len(sys.argv) != 3:
        print("Usage: python hr_production_sim.py <Total_Requests> <Concurrency>")
        print("Example: python hr_production_sim.py 5000 50")
        sys.exit(1)
        
    total_requests = int(sys.argv[1])
    pool_size = int(sys.argv[2])
    
    print(f"Initializing Production Pool (Size: {pool_size})...")
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="prod_pool",
            pool_size=pool_size,
            pool_reset_session=True,
            **DB_CONFIG
        )
    except Error as e:
        print(f"Failed to create pool: {e}")
        sys.exit(1)

    print(f"\n--- STARTING HRIS PRODUCTION SIMULATION ---")
    print(f"Workload        : 30 Real-World Business Scenarios")
    print(f"Requests        : {total_requests}")
    print(f"Concurrency     : {pool_size} threads")
    print("="*60)

    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures = [executor.submit(execute_request, connection_pool) for _ in range(total_requests)]
        
        completed = 0
        milestone = total_requests // 10 if total_requests >= 10 else 1
        
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % milestone == 0:
                print(f"Progress: {completed}/{total_requests} ({(completed/total_requests)*100:.0f}%)")

    end_time = time.time()
    total_duration = end_time - start_time
    
    # REPORTING
    successes = [r for r in results if r[0]]
    failures = [r for r in results if not r[0]]
    
    print("\n" + "="*60)
    print("SIMULATION REPORT")
    print("="*60)
    print(f"Duration          : {total_duration:.4f} s")
    print(f"Throughput (QPS)  : {(total_requests / total_duration):.2f}")
    print("-" * 60)
    print(f"Successful Trx    : {len(successes)}")
    print(f"Failed Trx        : {len(failures)}")
    
    if successes:
        print("\nBusiness Action Distribution (Top 10):")
        type_counts = {}
        for r in successes:
            name = r[1]
            type_counts[name] = type_counts.get(name, 0) + 1
        
        sorted_stats = sorted(type_counts.items(), key=lambda item: item[1], reverse=True)
        for name, count in sorted_stats[:10]:
            print(f" - {name:<25} : {count}")

    if failures:
        print("\nError Analysis:")
        err_counts = {}
        for f in failures:
            msg = f[2]
            err_counts[msg] = err_counts.get(msg, 0) + 1
        for err, count in list(err_counts.items())[:5]:
            print(f"[{count}x] {err}")
    print("="*60)

if __name__ == "__main__":
    main()
