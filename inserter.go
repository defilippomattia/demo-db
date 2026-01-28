package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type CommandFlags struct {
	ConfigPath string
	Insert     bool
	DropTables bool
	Recreate   bool
}

type InserterConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Inserter struct {
		Mode          string `json:"mode"`
		EveryNSeconds int    `json:"every_n_seconds"`
	} `json:"inserter"`
}

func parseAndValidateFlags() (*CommandFlags, error) {
	configPath := flag.String("config", "", "Path to config file")
	insert := flag.Bool("insert", false, "Insert data")
	dropTables := flag.Bool("drop-tables", false, "Drop all tables")
	recreate := flag.Bool("recreate", false, "Drop and recreate all tables and insert data")

	flag.Parse()

	if *configPath == "" {
		return nil, fmt.Errorf("--config is required")
	}

	actionCount := 0
	if *insert {
		actionCount++
	}
	if *dropTables {
		actionCount++
	}
	if *recreate {
		actionCount++
	}

	if actionCount == 0 {
		return nil, fmt.Errorf(
			"one action is required: --insert, --drop-tables, or --recreate",
		)
	}

	if actionCount > 1 {
		return nil, fmt.Errorf("only one action can be specified at a time")
	}

	return &CommandFlags{
		ConfigPath: *configPath,
		Insert:     *insert,
		DropTables: *dropTables,
		Recreate:   *recreate,
	}, nil
}

func loadConfig(path string) (*InserterConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open config file: %w", err)
	}
	defer file.Close()

	var cfg InserterConfig
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file: %w", err)
	}

	validModes := []string{
		"timestamp-only",
		"realistic-data",
		"gibberish-data",
	}

	modeValid := false
	for _, m := range validModes {
		if strings.ToLower(cfg.Inserter.Mode) == m {
			modeValid = true
			break
		}
	}
	if !modeValid {
		return nil, fmt.Errorf(
			"invalid inserter.mode '%s', must be one of %v",
			cfg.Inserter.Mode,
			validModes,
		)
	}

	return &cfg, nil
}

func connectDB(cfg *InserterConfig) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("cannot ping DB: %w", err)
	}

	return conn, nil
}

func dropTables(conn *pgx.Conn) error {
	tables := []string{
		"timestamp",
		"album",
		"artist",
		"customer",
		"employee",
		"genre",
		"invoice",
		"invoice_line",
		"media_type",
		"playlist",
		"playlist_track",
		"track",
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables specified for truncation")
	}

	quoted := make([]string, 0, len(tables))
	for _, t := range tables {
		quoted = append(quoted, fmt.Sprintf(`"%s"`, t))
	}

	query := fmt.Sprintf(
		"DROP TABLE %s",
		strings.Join(quoted, ", "),
	)

	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("dropping tables failed: %w", err)
	}

	return nil
}

func runInsert(cfg *InserterConfig, dbConn *pgx.Conn) {
	if strings.ToLower(cfg.Inserter.Mode) != "timestamp-only" {
		fmt.Println("Insert mode not supported yet.")
		return
	}

	fmt.Println("Running timestamp-only insert...")

	for {
		// truncate to seconds
		_, err := dbConn.Exec(
			context.Background(),
			`INSERT INTO "timestamp"(created_at) VALUES (NOW())`,
		)
		if err != nil {
			fmt.Println("Error inserting timestamp:", err)
			return
		}

		fmt.Println("Inserted current timestamp (seconds precision).")

		time.Sleep(time.Duration(cfg.Inserter.EveryNSeconds) * time.Second)
	}
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

	fmt.Println("Config loaded successfully, inserter mode:", cfg.Inserter.Mode)

	dbConn, err := connectDB(cfg)
	if err != nil {
		fmt.Println("Database connection failed:", err)
		return
	}
	defer dbConn.Close(context.Background())
	fmt.Println("Database connection successful!")

	switch {

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
			if err := dropTables(dbConn); err != nil {
				fmt.Println("Error while dropping tables:", err)
				return
			}

		} else {
			fmt.Println("Aborted. No rows were deleted.")
		}
	case flags.Recreate:
		fmt.Println("Recreating all tables...")
	}

}
