import sys
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from mysql.connector import Error, pooling

# ==========================================
# 1. CONFIGURATION
# ==========================================
DB_CONFIG = {
    'host': 'XXXX',
    'port': 3306,
    'user': 'readyset_repl',
    'password': 'XXXX',
    'database': 'employees',
    'connection_timeout': 5
}

# Data Constants for Randomization
VALID_DEPTS = ['d001', 'd002', 'd003', 'd004', 'd005', 'd006', 'd007', 'd008', 'd009']
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# ==========================================
# 2. FAST OLTP QUERIES (No Aggregates)
# ==========================================

def get_random_id():
    # Optimization: Random ID between 10001 and 300000 (Bulk of dataset)
    return random.randint(10001, 300000)

def q_login_check(cursor):
    """Simulates: User Login (Fetch ID and Name/Auth info)"""
    emp_id = get_random_id()
    cursor.execute("SELECT emp_no, first_name, last_name, hire_date FROM employees WHERE emp_no = %s", (emp_id,))
    cursor.fetchall()
    return "Login Check"

def q_view_profile(cursor):
    """Simulates: Loading a Full Employee Profile Page"""
    emp_id = get_random_id()
    query = """
        SELECT e.first_name, e.last_name, t.title, d.dept_name, s.salary
        FROM employees e
        JOIN titles t ON e.emp_no = t.emp_no
        JOIN dept_emp de ON e.emp_no = de.emp_no
        JOIN departments d ON de.dept_no = d.dept_no
        JOIN salaries s ON e.emp_no = s.emp_no
        WHERE e.emp_no = %s 
        AND t.to_date > NOW() 
        AND s.to_date > NOW()
        LIMIT 1
    """
    cursor.execute(query, (emp_id,))
    cursor.fetchall()
    return "View Profile"

def q_latest_salary(cursor):
    """Simulates: Employee checking current Pay Stub"""
    emp_id = get_random_id()
    query = """
        SELECT salary, from_date, to_date 
        FROM salaries 
        WHERE emp_no = %s 
        ORDER BY from_date DESC LIMIT 1
    """
    cursor.execute(query, (emp_id,))
    cursor.fetchall()
    return "Check Salary"

def q_dept_colleagues(cursor):
    """Simulates: Viewing 'My Team' (Peers in same department)"""
    dept = random.choice(VALID_DEPTS)
    # LIMIT 10 ensures we don't fetch 50k rows, keeping it fast like a UI page
    query = """
        SELECT e.first_name, e.last_name 
        FROM employees e
        JOIN dept_emp de ON e.emp_no = de.emp_no
        WHERE de.dept_no = %s AND de.to_date > NOW()
        LIMIT 10
    """
    cursor.execute(query, (dept,))
    cursor.fetchall()
    return "Team View"

def q_job_history(cursor):
    """Simulates: Viewing Promotion/Title History"""
    emp_id = get_random_id()
    cursor.execute("SELECT title, from_date, to_date FROM titles WHERE emp_no = %s", (emp_id,))
    cursor.fetchall()
    return "Job History"

def q_manager_lookup(cursor):
    """Simulates: Looking up the Manager of a Department"""
    dept = random.choice(VALID_DEPTS)
    query = """
        SELECT e.first_name, e.last_name 
        FROM employees e
        JOIN dept_manager dm ON e.emp_no = dm.emp_no
        WHERE dm.dept_no = %s AND dm.to_date > NOW()
    """
    cursor.execute(query, (dept,))
    cursor.fetchall()
    return "Manager Lookup"

def q_search_by_name(cursor):
    """Simulates: HR Search Bar (Autocomplete)"""
    # Searching 3 letter prefix is common in UI autocomplete
    prefix = random.choice(ALPHABET) + random.choice(ALPHABET) + random.choice(ALPHABET)
    cursor.execute("SELECT emp_no, first_name, last_name FROM employees WHERE last_name LIKE CONCAT(%s, '%') LIMIT 5", (prefix,))
    cursor.fetchall()
    return "Search Name"

def q_recent_hires(cursor):
    """Simulates: 'New Joiners' widget on Dashboard"""
    # Simple indexed range scan on hire_date
    year = random.randint(1990, 1999)
    cursor.execute("SELECT first_name, last_name FROM employees WHERE hire_date BETWEEN %s AND %s LIMIT 5", (f"{year}-01-01", f"{year}-12-31"))
    cursor.fetchall()
    return "Recent Hires"

# The pool of transaction types
QUERY_POOL = [
    q_login_check, 
    q_view_profile, 
    q_latest_salary, 
    q_dept_colleagues, 
    q_job_history, 
    q_manager_lookup, 
    q_search_by_name, 
    q_recent_hires
]

# ==========================================
# 3. WORKER EXECUTION
# ==========================================

def execute_request(connection_pool):
    """
    Acquires connection, picks random query, executes, releases connection.
    """
    connection = None
    cursor = None
    query_name = "Unknown"
    
    try:
        connection = connection_pool.get_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            
            # RANDOMLY PICK A QUERY TYPE
            # This happens inside the thread, ensuring true randomness per request
            query_func = random.choice(QUERY_POOL)
            
            # Execute
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
        print("Usage: python hr_fast_stress.py <Total_Requests> <Concurrency_Threads>")
        print("Example: python hr_fast_stress.py 5000 50")
        sys.exit(1)
        
    total_requests = int(sys.argv[1])
    pool_size = int(sys.argv[2])
    
    print(f"Initializing Connection Pool (Size: {pool_size})...")
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="fast_pool",
            pool_size=pool_size,
            pool_reset_session=True,
            **DB_CONFIG
        )
    except Error as e:
        print(f"Failed to create pool: {e}")
        sys.exit(1)

    print(f"\n--- STARTING FAST OLTP STRESS TEST ---")
    print(f"Total Requests  : {total_requests}")
    print(f"Concurrency     : {pool_size} threads")
    print("Mode            : Unlimited QPS (Max Throughput)")
    print("Query Types     : 8 Transactional Patterns (No Aggregates)")
    print("="*60)

    start_time = time.time()
    results = []
    
    # Execute requests using ThreadPool
    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures = [executor.submit(execute_request, connection_pool) for _ in range(total_requests)]
        
        # Progress Bar
        completed = 0
        milestone = total_requests // 10 if total_requests >= 10 else 1
        
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % milestone == 0:
                print(f"Progress: {completed}/{total_requests} ({(completed/total_requests)*100:.0f}%)")

    end_time = time.time()
    total_duration = end_time - start_time
    
    # ==========================================
    # 4. REPORTING
    # ==========================================
    successes = [r for r in results if r[0]]
    failures = [r for r in results if not r[0]]
    
    print("\n" + "="*60)
    print("FINAL PERFORMANCE REPORT")
    print("="*60)
    print(f"Requests Requested   : {total_requests}")
    print(f"Concurrency Level    : {pool_size}")
    print(f"Total Time Elapsed   : {total_duration:.4f} seconds")
    print("-" * 60)
    print(f"THROUGHPUT (QPS)     : {(total_requests / total_duration):.2f} queries/sec")
    print(f"Avg Latency          : {(total_duration * 1000 / total_requests):.2f} ms")
    print("-" * 60)
    print(f"Successful Queries   : {len(successes)}")
    print(f"Failed Queries       : {len(failures)}")
    
    # Distribution of query types run
    if successes:
        print("\nQuery Distribution (What was run?):")
        type_counts = {}
        for r in successes:
            name = r[1]
            type_counts[name] = type_counts.get(name, 0) + 1
        
        for name, count in sorted(type_counts.items(), key=lambda item: item[1], reverse=True):
            print(f" - {name:<15} : {count}")

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
