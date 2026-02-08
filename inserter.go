package main

import (
	"bufio"
	"context"
	"embed"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func dropTables(ctx context.Context, cfg *InserterConfig, pool *pgxpool.Pool) error {
	tables := []string{
		"timestamp", "album", "artist", "customer", "employee",
		"playlist", "playlist_track", "track", "genre", "media_type", "invoice", "invoice_line", "bigtable",
	}

	batch := &pgx.Batch{}
	for _, t := range tables {
		query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s" CASCADE`, t)
		batch.Queue(query)
	}
	results := pool.SendBatch(ctx, batch)
	defer results.Close()

	for _, t := range tables {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("dropping table %s failed: %w", t, err)
		}
		fmt.Printf("Dropped table %s (if existed)\n", t)
	}

	return nil
}

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GenerateRandomString(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = alphabet[rand.Uint64N(uint64(len(alphabet)))]
	}
	return string(result)
}

func runInsert(cfg *InserterConfig, pool *pgxpool.Pool) {
	//todo: refactor
	var wg sync.WaitGroup

	if cfg.Inserter.TimestampInserts.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Printf("Running timestamp inserts every %d seconds...\n", cfg.Inserter.TimestampInserts.EveryNSeconds)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := pool.Exec(ctx, `INSERT INTO "timestamp"(created_at) VALUES (NOW())`)
				cancel()
				if err != nil {
					fmt.Println("Error inserting timestamp:", err)
				}
				time.Sleep(time.Duration(cfg.Inserter.TimestampInserts.EveryNSeconds) * time.Second)
			}
		}()
	}

	if cfg.Inserter.BigTableInserts.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			randStr := GenerateRandomString(120)

			for {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := pool.Exec(ctx, `
					INSERT INTO "bigtable"(cola, colb, colc, cold, cole) VALUES ($1, $2, $3, $4, $5)`,
					randStr,
					randStr,
					randStr,
					randStr,
					randStr,
				)
				cancel()
				if err != nil {
					fmt.Println("Error inserting into bigtable:", err)
				}
			}
		}()
	}

	if cfg.Inserter.MainTablesInserts.Enabled {
		fmt.Println("Inserting gibberish data into tables...")

		var wg sync.WaitGroup
		wg.Add(5)

		fmt.Println("inserting gibberish data into artist table...")
		go func() {
			defer wg.Done()
			for {
				randStr := GenerateRandomString(20)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := pool.Exec(ctx, `INSERT INTO "artist"(name) VALUES ($1)`, randStr)
				cancel()
				if err != nil {
					fmt.Println("Error inserting gibberish data into artist table:", err)
					return
				}
			}
		}()

		fmt.Println("inserting gibberish data into genre table...")
		go func() {
			defer wg.Done()
			for {
				randStr := GenerateRandomString(120)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := pool.Exec(ctx, `INSERT INTO "genre"(name) VALUES ($1)`, randStr)
				cancel()
				if err != nil {
					fmt.Println("Error inserting gibberish data into genre table:", err)
					return
				}
			}
		}()

		fmt.Println("inserting gibberish data into media_type table...")
		go func() {
			defer wg.Done()
			for {
				randStr := GenerateRandomString(120)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := pool.Exec(ctx, `INSERT INTO "media_type"(name) VALUES ($1)`, randStr)
				cancel()
				if err != nil {
					fmt.Println("Error inserting gibberish data into media_type table:", err)
					return
				}
			}
		}()

		fmt.Println("inserting gibberish data into playlist table...")
		go func() {
			defer wg.Done()
			for {
				randStr := GenerateRandomString(120)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := pool.Exec(ctx, `INSERT INTO "playlist"(name) VALUES ($1)`, randStr)
				cancel()
				if err != nil {
					fmt.Println("Error inserting gibberish data into playlist table:", err)
					return
				}
			}
		}()

		fmt.Println("inserting gibberish data into employee table...")
		go func() {
			defer wg.Done()
			for {
				len20RandStr := GenerateRandomString(20)
				len40RandStr := GenerateRandomString(40)
				len60RandStr := GenerateRandomString(60)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := pool.Exec(ctx, `
					INSERT INTO "employee" (
						last_name, first_name, title, address, city, 
						state, country, phone, fax, email
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
					len20RandStr, // last_name
					len20RandStr, // first_name
					len20RandStr, // title
					len60RandStr, // address
					len40RandStr, // city
					len40RandStr, // state
					len40RandStr, // country
					len20RandStr, // phone
					len20RandStr, // fax
					len60RandStr, // email
				)
				cancel()

				if err != nil {
					fmt.Println("Error inserting gibberish data into employee table:", err)
					return
				}

			}
		}()

		wg.Wait()

	}

}

//go:embed 00-create-tables.sql 01-insert-data.sql
var embeddedSqlFiles embed.FS

func executeSqlFiles(pool *pgxpool.Pool, sqlFiles []string) error {
	for _, file := range sqlFiles {
		content, err := embeddedSqlFiles.ReadFile(file)
		if err != nil {
			return fmt.Errorf("error reading SQL file %s: %w", file, err)
		}

		_, err = pool.Exec(context.Background(), string(content))
		if err != nil {
			return fmt.Errorf("error executing SQL file %s: %w", file, err)
		}
		fmt.Printf("Executed SQL file %s successfully.\n", file)
	}
	return nil
}

func main() {
	flags, err := parseAndValidateFlags()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	cfg, err := loadConfig(flags.ConfigPath)
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}

	// fmt.Println("Config loaded successfully, inserter mode:", cfg.Inserter.Mode)

	dbConn, err := connectPool(cfg)
	if err != nil {
		fmt.Println("Database connection failed:", err)
		return
	}
	defer dbConn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch {
	case flags.Validate:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dbConn.Ping(ctx); err != nil {
			fmt.Printf("validation failed: could not connect to database, error: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("validation successful: config is valid and database connection established.")

	case flags.Insert:
		fmt.Println("Running insert...")
		runInsert(cfg, dbConn)

	case flags.DropTables:
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Are you sure you want to drop all tables? (yes/no): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))

		if input == "yes" || input == "y" {
			fmt.Println("Dropping all tables...")
			if err := dropTables(ctx, cfg, dbConn); err != nil {
				fmt.Println("Error while dropping tables:", err)
				return
			}
		} else {
			fmt.Println("Aborted. No rows were deleted.")
		}

	case flags.Recreate:
		fmt.Println("Recreating all tables...")
		if err := executeSqlFiles(dbConn, []string{"00-create-tables.sql", "01-insert-data.sql"}); err != nil {
			fmt.Println("Error while recreating tables:", err)
			return
		}
		fmt.Println("Recreation completed successfully.")

	case flags.CreateTables:
		fmt.Println("Creating tables without inserting data...")
		if err := executeSqlFiles(dbConn, []string{"00-create-tables.sql"}); err != nil {
			fmt.Println("Error while creating tables:", err)
			return
		}
		fmt.Println("Tables created successfully.")
	}
}
