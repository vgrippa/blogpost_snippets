package main

import (
    "database/sql"
    "fmt"
    "log"
    "math/rand"
    "os"
    "sort"
    "strconv"
    "sync"
    "time"

    _ "github.com/go-sql-driver/mysql"
)

// --- 1. CONFIGURATION ---
const (
    DBUser = "readyset_repl"
    DBPass = "readyset_repl"
    DBHost = "52.91.141.5"
//    DBHost = "vinitest-2ecf8437dd592adf937b5261.readyset.cloud"
    DBPort = "3306"
    DBName = "employees"
)

// CRM Constants
const ActiveDate = "9999-01-01"
var Departments = []string{"d001", "d002", "d003", "d004", "d005", "d009"} // Sales, Marketing, HR, etc.

// --- 2. DATA HELPERS ---

func getUserID() int {
    // Simulate active employees (IDs 10001 to 40000)
    return rand.Intn(30000) + 10001
}

func getRandomDept() string {
    return Departments[rand.Intn(len(Departments))]
}

// --- 3. CRM MODULES & QUERIES ---

type CRMAction func(*sql.DB) string

// === MODULE: AUTHENTICATION ===

// 1. Login Check
// Simulates the initial query when any user opens the CRM.
func mod_Auth_Login(db *sql.DB) string {
    id := getUserID()
    // Fetching password hash (simulated) and salt
    query := "SELECT first_name, last_name, hire_date FROM employees WHERE emp_no = ?"
    _, err := db.Exec(query, id)
    if err != nil { return "Error" }
    return "[Auth] Login"
}

// 2. Session Validate
// Checks if the user is still active in the system.
func mod_Auth_Session(db *sql.DB) string {
    id := getUserID()
    query := "SELECT dept_no FROM dept_emp WHERE emp_no = ? AND to_date = ?"
    _, err := db.Exec(query, id, ActiveDate)
    if err != nil { return "Error" }
    return "[Auth] Validate Session"
}

// === MODULE: SALES DASHBOARD ===

// 3. View My Profile
// Sales reps constantly checking their own dashboard.
func mod_Sales_Profile(db *sql.DB) string {
    id := getUserID()
    query := `SELECT e.first_name, t.title, s.salary
              FROM employees e
              JOIN titles t ON e.emp_no = t.emp_no
              JOIN salaries s ON e.emp_no = s.emp_no
              WHERE e.emp_no = ? AND t.to_date = ? AND s.to_date = ?`
    _, err := db.Exec(query, id, ActiveDate, ActiveDate)
    if err != nil { return "Error" }
    return "[Sales] View Profile"
}

// 4. Commission Check (Current Salary)
// Checking current compensation.
func mod_Sales_Salary(db *sql.DB) string {
    id := getUserID()
    _, err := db.Exec("SELECT salary FROM salaries WHERE emp_no = ? AND to_date = ?", id, ActiveDate)
    if err != nil { return "Error" }
    return "[Sales] Check Commission"
}

// 5. Find Team Lead
// Looking up the manager of their current department.
func mod_Sales_FindManager(db *sql.DB) string {
    dept := getRandomDept()
    query := "SELECT emp_no FROM dept_manager WHERE dept_no = ? AND to_date = ?"
    _, err := db.Exec(query, dept, ActiveDate)
    if err != nil { return "Error" }
    return "[Sales] Find Team Lead"
}

// === MODULE: HR & ADMIN ===

// 6. Employee History Lookup
// HR checking a user's title progression.
func mod_HR_TitleHistory(db *sql.DB) string {
    id := getUserID()
    _, err := db.Exec("SELECT title, from_date, to_date FROM titles WHERE emp_no = ?", id)
    if err != nil { return "Error" }
    return "[HR] Title History"
}

// 7. Department Roster (Active)
// Listing active employees in a specific department.
func mod_HR_DeptRoster(db *sql.DB) string {
    dept := getRandomDept()
    // Limit ensures we don't pull 50k rows, simulating a paginated UI
    query := "SELECT emp_no FROM dept_emp WHERE dept_no = ? AND to_date = ? LIMIT 20"
    _, err := db.Exec(query, dept, ActiveDate)
    if err != nil { return "Error" }
    return "[HR] Dept Roster"
}

// 8. New Hire Check
// Checking for employees hired recently (simulate last known hires).
func mod_HR_NewHires(db *sql.DB) string {
    // Searching for hires after a specific date
    _, err := db.Exec("SELECT emp_no, first_name FROM employees WHERE hire_date > '1999-10-01' LIMIT 10")
    if err != nil { return "Error" }
    return "[HR] New Hires Report"
}

// 9. Employee Search (By Name)
// Admin searching for a user.
func mod_Admin_SearchName(db *sql.DB) string {
    // Search for 'Geo' prefix
    _, err := db.Exec("SELECT emp_no, last_name FROM employees WHERE first_name = 'Georgi'")
    if err != nil { return "Error" }
    return "[Admin] Search User"
}

// 10. System Health Check
// A heartbeat check performed by the CRM system itself.
func mod_Sys_Heartbeat(db *sql.DB) string {
    _, err := db.Exec("SELECT 1")
    if err != nil { return "Error" }
    return "[Sys] Heartbeat"
}

// --- 4. EXECUTION ENGINE ---

func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: go run main.go <TotalRequests> <Concurrency>")
        fmt.Println("Example: go run main.go 5000 50")
        return
    }

    totalRequests, _ := strconv.Atoi(os.Args[1])
    concurrency, _ := strconv.Atoi(os.Args[2])

    // A. SETUP CONNECTION POOL
    dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DBUser, DBPass, DBHost, DBPort, DBName)
    db, err := sql.Open("mysql", dsn)
    if err != nil { log.Fatal(err) }

    // CRITICAL: Configure the pool to handle the concurrency
    db.SetMaxOpenConns(concurrency)
    db.SetMaxIdleConns(concurrency)
    db.SetConnMaxLifetime(time.Minute * 5)

    if err := db.Ping(); err != nil { log.Fatal("Cannot connect to DB:", err) }
    defer db.Close()

    // B. DEFINE WORKLOAD MIX
    actions := []CRMAction{
        mod_Auth_Login, mod_Auth_Login, // Login happens frequently
        mod_Auth_Session,
        mod_Sales_Profile, mod_Sales_Profile, // Sales team is very active
        mod_Sales_Salary,
        mod_Sales_FindManager,
        mod_HR_TitleHistory,
        mod_HR_DeptRoster,
        mod_HR_NewHires,
        mod_Admin_SearchName,
        mod_Sys_Heartbeat,
    }

    fmt.Printf("\n--- STARTING CRM SIMULATION ---\n")
    fmt.Printf("Scenario    : Multi-Department Usage (Sales, HR, Admin)\n")
    fmt.Printf("Requests    : %d\n", totalRequests)
    fmt.Printf("Users (Thr) : %d\n", concurrency)
    fmt.Println("==================================================")

    // C. RUN WORKERS
    var wg sync.WaitGroup
    jobs := make(chan int, totalRequests)
    results := make(chan string, totalRequests)

    start := time.Now()

    for w := 0; w < concurrency; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for range jobs {
                // Pick random CRM action
                action := actions[rand.Intn(len(actions))]
                res := action(db)
                results <- res
            }
        }()
    }

    go func() {
        for i := 0; i < totalRequests; i++ {
            jobs <- i
        }
        close(jobs)
    }()

    go func() {
        wg.Wait()
        close(results)
    }()

    // D. COLLECT METRICS
    stats := make(map[string]int)
    success := 0
    fail := 0

    for res := range results {
        if res == "Error" {
            fail++
        } else {
            success++
            stats[res]++
        }
    }

    duration := time.Since(start)

    // E. REPORT
    fmt.Printf("\n==================================================\n")
    fmt.Printf("CRM PERFORMANCE REPORT\n")
    fmt.Printf("==================================================\n")
    fmt.Printf("Total Time       : %.4f s\n", duration.Seconds())
    fmt.Printf("Throughput (QPS) : %.2f\n", float64(totalRequests)/duration.Seconds())
    fmt.Printf("Successful       : %d\n", success)
    fmt.Printf("Errors           : %d\n", fail)

    fmt.Println("\nModule Usage Distribution:")

    // Sort for cleaner output
    type kv struct { Key string; Value int }
    var ss []kv
    for k, v := range stats { ss = append(ss, kv{k, v}) }
    sort.Slice(ss, func(i, j int) bool { return ss[i].Value > ss[j].Value })

    for _, kv := range ss {
        fmt.Printf("- %-25s : %d\n", kv.Key, kv.Value)
    }
    fmt.Println("==================================================")
}
