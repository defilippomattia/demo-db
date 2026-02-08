package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type CommandFlags struct {
	ConfigPath   string
	Insert       bool
	DropTables   bool
	Recreate     bool
	Validate     bool
	CreateTables bool
}

// type InserterConfig struct {
// 	Host     string `json:"host"`
// 	Port     string `json:"port"`
// 	Database string `json:"database"`
// 	Username string `json:"username"`
// 	Password string `json:"password"`
// 	Inserter struct {
// 		Mode          string `json:"mode"`
// 		EveryNSeconds int    `json:"every_n_seconds"`
// 	} `json:"inserter"`
// }

type InserterConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Inserter struct {
		WalSwitcher struct {
			Enabled       bool `json:"enabled"`
			EveryNSeconds int  `json:"every_n_seconds"`
		} `json:"wal_switcher"`
		TimestampInserts struct {
			Enabled       bool `json:"enabled"`
			EveryNSeconds int  `json:"every_n_seconds"`
		} `json:"timestamp_inserts"`
		BigTableInserts struct {
			Enabled       bool `json:"enabled"`
			EveryNSeconds int  `json:"every_n_seconds"`
		} `json:"bigtable_inserts"`
		MainTablesInserts struct {
			Mode          string `json:"mode"`
			Enabled       bool   `json:"enabled"`
			EveryNSeconds int    `json:"every_n_seconds"`
		} `json:"main_tables_inserts"`
	} `json:"inserter"`
}

func parseAndValidateFlags() (*CommandFlags, error) {
	configPath := flag.String("config", "", "Path to config file")
	insert := flag.Bool("insert", false, "Insert data")
	dropTables := flag.Bool("drop-tables", false, "Drop all tables")
	recreate := flag.Bool("recreate", false, "Drop and recreate all tables and insert data")
	validate := flag.Bool("validate", false, "Validate database connection and config")
	createTables := flag.Bool("create-tables", false, "Create tables without inserting data")

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
	if *validate {
		actionCount++
	}

	if *createTables {
		actionCount++
	}

	if actionCount == 0 {
		return nil, fmt.Errorf("one action is required: --insert, --create-tables, --drop-tables, --validate or --recreate")
	}
	if actionCount > 1 {
		return nil, fmt.Errorf("only one action can be specified at a time")
	}

	return &CommandFlags{
		ConfigPath:   *configPath,
		Insert:       *insert,
		DropTables:   *dropTables,
		Recreate:     *recreate,
		Validate:     *validate,
		CreateTables: *createTables,
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

	// validModes := []string{"timestamp-only", "realistic-data", "gibberish-data"}
	// modeValid := false
	// for _, m := range validModes {
	// 	if strings.ToLower(cfg.Inserter.Mode) == m {
	// 		modeValid = true
	// 		break
	// 	}
	// }

	// if !modeValid {
	// 	return nil, fmt.Errorf("invalid inserter.mode '%s', must be one of %v", cfg.Inserter.Mode, validModes)
	// }

	return &cfg, nil
}
