package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {

	connString := getEnv("PG_CONN", "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable")

	const (
		query = "SELECT 1;"
	)

	if firstTestConnection, err := sql.Open("pgx", connString); err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	} else if err = firstTestConnection.Ping(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	} else if _, err := firstTestConnection.Query(query); err != nil {
		log.Fatalf("Query error: %v", err)
		return
	} else if err = firstTestConnection.Close(); err != nil {
		log.Fatalf("Cloud not close: %v", err)
	}

	ctx := context.Background()

	db, err := sql.Open("pgx", connString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Get 2 dedicated physical connections from the pool
	conn1, err := db.Conn(ctx) //
	if err != nil {
		log.Fatal(err)
	}
	defer conn1.Close()

	conn2, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn2.Close()

	start := time.Now()

	logSinceElapsed := func(format string, args ...any) {
		elapsed := time.Since(start).Milliseconds()
		fmt.Printf("%dms  %s\n", elapsed, fmt.Sprintf(format, args...))
	}

	// fmt.Println("CONN1: SET TRANSACTION SERIALIZABLE;")
	// conn1.ExecContext(ctx, "SET TRANSACTION SERIALIZABLE;")

	// fmt.Println("CONN2: SET TRANSACTION SERIALIZABLE;")
	// conn2.ExecContext(ctx, "SET TRANSACTION SERIALIZABLE;")

	logSinceElapsed("T1: BEGIN;")
	transaction1, _ := conn1.BeginTx(ctx, nil)

	//Lock row
	row := transaction1.QueryRowContext(ctx, "SELECT AVG(price) AS avgprice FROM gotest.products;")
	var avgprice float64
	if err := row.Scan(&avgprice); err != nil {
		log.Fatal(err)
	}
	fmt.Println("T1: avgprice: ", avgprice)

	logSinceElapsed("T1: SELECT * FROM gotest.products FOR UPDATE;")
	transaction1.ExecContext(ctx, "SELECT * FROM gotest.products FOR UPDATE;")

	//Try select
	go func() {
		logSinceElapsed("T2: BEGIN;")
		transaction2, _ := conn2.BeginTx(ctx, nil)

		logSinceElapsed("T2: SELECT AVG(price) AS avgprice FROM gotest.products;")
		row := transaction2.QueryRowContext(ctx, "SELECT AVG(price) AS avgprice FROM gotest.products;")
		var avgprice float64
		if err := row.Scan(&avgprice); err != nil {
			log.Fatal(err)
		}
		logSinceElapsed("T2 OUTPUT: avgprice = %f", avgprice)

		transaction2.Commit()
	}()

	//Update
	logSinceElapsed("T1: UPDATE gotest.products SET price = 5000 where product_id = 1;")
	transaction1.ExecContext(ctx, "UPDATE gotest.products SET price = 5000 where product_id = 1;")

	logSinceElapsed("T1: sleeping 10s');")
	time.Sleep(10 * time.Second)

	logSinceElapsed("T1: COMMIT;")
	transaction1.Commit()
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func RowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		rowMap := make(map[string]interface{}, len(cols))
		for i, colName := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	return results, nil
}
