package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Constant block
const (
	SQL1 = "insert into csse_head "
	SQL2 = "UID, iso2, iso3, code3, FIPS, County, State, Country, Latitude, Longitude, StateCountry"
)

func main() {
	fptr := flag.String("fpath", "covid.txt", "file path to read from")
	flag.Parse()

	pool, err := sql.Open("mysql", "")
	if err != nil {
		panic(err.Error())
	}
	pool.SetConnMaxLifetime(0)
	pool.SetMaxIdleConns(3)
	pool.SetMaxOpenConns(3)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	con, err := pool.Conn(ctx)
	if err != nil {
		panic(err.Error())
	}

	dbTruncateCovid19(ctx, pool)
	// time.Sleep(4 * time.Second)

	f, err := os.Open(*fptr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	var line string
	var tokens []string
	s := bufio.NewScanner(f)

	// strip off field specifications
	s.Scan()

	var lastID int64
	idx := 0
	sqlInsert := ""
	for s.Scan() {
		line = strings.ReplaceAll(s.Text(), ", ", " - ")
		tokens = strings.Split(line, ",")

		for i := 0; i <= 10; i++ {
			switch i {
			case 0:
				sqlInsert = sqlInsert + SQL1 + "(" + SQL2 + ") select " + tokens[i]
			case 4:
				if len(tokens[i]) == 0 {
					sqlInsert = sqlInsert + ",0"
				} else {
					sqlInsert = sqlInsert + "," + tokens[i]
				}
			case 1, 2, 5, 6, 7:
				sqlInsert = sqlInsert + ",\"" + tokens[i] + "\""
			case 10:
				// Ignore the "combined_keys" field, some values contain commas...
				tokens[i] = "\"\""
				fallthrough
			default:
				sqlInsert = sqlInsert + "," + tokens[i]
			}
		}
		sqlInsert = sqlInsert + ";"
		// fmt.Println("<", sqlInsert, ">")
		lastID = dbInsertRowCovid19(ctx, pool, sqlInsert)
		fmt.Println("LastID value: ", lastID)

		idx++
		sqlInsert = ""
	}
	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}
	con.Close()
	pool.Close()
}

func dbTruncateCovid19(ctx context.Context, db *sql.DB) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := db.ExecContext(ctx, "truncate table csse_head;")
	if err != nil {
		log.Fatal("unable to truncate csse_head table", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Truncate successful: ", rows)
}

func dbInsertRowCovid19(ctx context.Context, db *sql.DB, sqlInsert string) (id int64) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := db.ExecContext(ctx, sqlInsert)
	if err != nil {
		log.Fatal("unable to insert into csse_head table", err)
	}
	/*
		rows, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
	*/
	lastID, err := result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	return lastID
}

func dbInsertRowCovid19TX(ctx context.Context, db *sql.DB, sqlInsert string) (id int64) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Fatal(err)
	}
	result, execErr := tx.ExecContext(ctx, sqlInsert)
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatalf("update failed: %v, unable to rollback: %v\n", execErr, rollbackErr)
		}
		log.Fatalf("update failed: %v", execErr)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	lastID, err := result.LastInsertId()
	return lastID
}
