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
    DBPass = "XXXX"
    DBHost = "XXXX"
//    DBHost = "XXX.readyset.cloud"
    DBPort = "3306"
    DBName = "employees"
)

const ActiveDate = "9999-01-01"
var Departments = []string{"d001", "d002", "d003", "d004", "d005", "d009"}

// --- 2. HELPERS ---

func getUserID() int {
    return rand.Intn(30000) + 10001
}

func getRandomDept() string {
    return Departments[rand.Intn(len(Departments))]
}

// --- 3. CRM MODULES ---

type CRMAction func(*sql.DB) string

// [Auth] Login
func mod_Auth_Login(db *sql.DB) string {
    id := getUserID()
    query := "SELECT first_name, last_name, hire_date FROM employees WHERE emp_no = ?"
    _, err := db.Exec(query, id)
    if err != nil { return "Error" }
    return "[Auth] Login"
}

// [Auth] Session
func mod_Auth_Session(db *sql.DB) string {
    id := getUserID()
    query := "SELECT dept_no FROM dept_emp WHERE emp_no = ? AND to_date = ?"
    _, err := db.Exec(query, id, ActiveDate)
    if err != nil { return "Error" }
    return "[Auth] Validate Session"
}

// [Sales] Profile
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

// [Sales] Salary
func mod_Sales_Salary(db *sql.DB) string {
    id := getUserID()
    _, err := db.Exec("SELECT salary FROM salaries WHERE emp_no = ? AND to_date = ?", id, ActiveDate)
    if err != nil { return "Error" }
    return "[Sales] Check Commission"
}

// [Sales] Manager
func mod_Sales_FindManager(db *sql.DB) string {
    dept := getRandomDept()
    query := "SELECT emp_no FROM dept_manager WHERE dept_no = ? AND to_date = ?"
    _, err := db.Exec(query, dept, ActiveDate)
    if err != nil { return "Error" }
    return "[Sales] Find Team Lead"
}

// [HR] History
func mod_HR_TitleHistory(db *sql.DB) string {
    id := getUserID()
    _, err := db.Exec("SELECT title, from_date, to_date FROM titles WHERE emp_no = ?", id)
    if err != nil { return "Error" }
    return "[HR] Title History"
}

// [HR] Roster
func mod_HR_DeptRoster(db *sql.DB) string {
    dept := getRandomDept()
    query := "SELECT emp_no FROM dept_emp WHERE dept_no = ? AND to_date = ? LIMIT 20"
    _, err := db.Exec(query, dept, ActiveDate)
    if err != nil { return "Error" }
    return "[HR] Dept Roster"
}

// [HR] New Hires
func mod_HR_NewHires(db *sql.DB) string {
    _, err := db.Exec("SELECT emp_no, first_name FROM employees WHERE hire_date > '1999-10-01' LIMIT 10")
    if err != nil { return "Error" }
    return "[HR] New Hires Report"
}

// [Admin] Search
func mod_Admin_SearchName(db *sql.DB) string {
    _, err := db.Exec("SELECT emp_no, last_name FROM employees WHERE first_name = 'Georgi'")
    if err != nil { return "Error" }
    return "[Admin] Search User"
}

// [Sys] Heartbeat
func mod_Sys_Heartbeat(db *sql.DB) string {
    _, err := db.Exec("SELECT 1")
    if err != nil { return "Error" }
    return "[Sys] Heartbeat"
}

// --- 4. EXECUTION ENGINE ---

func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: go run main.go <TotalRequests> <Concurrency>")
        return
    }

    totalRequests, _ := strconv.Atoi(os.Args[1])
    concurrency, _ := strconv.Atoi(os.Args[2])

    // A. SETUP CONNECTION POOL
    dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DBUser, DBPass, DBHost, DBPort, DBName)

    // Open connection object
    db, err := sql.Open("mysql", dsn)
    if err != nil { log.Fatal(err) }

    // CONFIGURE POOLING
    db.SetMaxOpenConns(concurrency)
    db.SetMaxIdleConns(concurrency)
    db.SetConnMaxLifetime(time.Minute * 5)

    // Verify connection
    if err := db.Ping(); err != nil { log.Fatal("Cannot connect to DB:", err) }
    defer db.Close()

    actions := []CRMAction{
        mod_Auth_Login, mod_Auth_Login,
        mod_Auth_Session,
        mod_Sales_Profile, mod_Sales_Profile,
        mod_Sales_Salary,
        mod_Sales_FindManager,
        mod_HR_TitleHistory,
        mod_HR_DeptRoster,
        mod_HR_NewHires,
        mod_Admin_SearchName,
        mod_Sys_Heartbeat,
    }

    fmt.Printf("\n--- STARTING POOLED CRM SIMULATION ---\n")
    fmt.Printf("Requests    : %d\n", totalRequests)
    fmt.Printf("Concurrency : %d\n", concurrency)
    fmt.Println("Strategy    : Persistent Connection Pool (Reuse)")
    fmt.Println("==================================================")

    var wg sync.WaitGroup
    jobs := make(chan int, totalRequests)
    results := make(chan string, totalRequests)

    start := time.Now()

    // Spawn Workers
    for w := 0; w < concurrency; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for range jobs {
                action := actions[rand.Intn(len(actions))]
                // Pass the pooled db object
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

    fmt.Printf("\n==================================================\n")
    fmt.Printf("CRM POOLED PERFORMANCE\n")
    fmt.Printf("==================================================\n")
    fmt.Printf("Total Time       : %.4f s\n", duration.Seconds())
    fmt.Printf("Throughput (QPS) : %.2f\n", float64(totalRequests)/duration.Seconds())
    fmt.Printf("Successful       : %d\n", success)
    fmt.Printf("Errors           : %d\n", fail)

    fmt.Println("\nModule Usage Distribution:")

    type kv struct { Key string; Value int }
    var ss []kv
    for k, v := range stats { ss = append(ss, kv{k, v}) }
    sort.Slice(ss, func(i, j int) bool { return ss[i].Value > ss[j].Value })

    for _, kv := range ss {
        fmt.Printf("- %-25s : %d\n", kv.Key, kv.Value)
    }
    fmt.Println("==================================================")
}
