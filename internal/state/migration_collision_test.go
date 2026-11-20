package state

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestMigrationCollisions(t *testing.T) {
	for _, kind := range []string{"state", "cache"} {
		for _, origin := range []string{"fresh", "fork", "upstream"} {
			t.Run(kind+"/"+origin, func(t *testing.T) {
				db, err := OpenDB(t.TempDir() + "/test.db")
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if origin != "fresh" {
					dir := "migrations/" + kind
					limit := 9
					if kind == "cache" {
						limit = 2
					}
					for i := 1; i <= limit; i++ {
						entries, err := migrationsFS.ReadDir(dir)
						if err != nil {
							t.Fatal(err)
						}
						prefix := fmt.Sprintf("%06d_", i)
						for _, e := range entries {
							name := e.Name()
							if len(name) >= len(prefix)+7 && name[:len(prefix)] == prefix && name[len(name)-7:] == ".up.sql" {
								b, err := migrationsFS.ReadFile(dir + "/" + name)
								if err != nil {
									t.Fatal(err)
								}
								if _, err = db.Exec(string(b)); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					var script string
					if kind == "state" && origin == "fork" {
						script = "ALTER TABLE platforms ADD COLUMN max_reference_latency_ms REAL NOT NULL DEFAULT 0"
					}
					if kind == "state" && origin == "upstream" {
						script = "ALTER TABLE subscriptions ADD COLUMN chain_proxy INTEGER NOT NULL DEFAULT 0; ALTER TABLE subscriptions ADD COLUMN chain_relay INTEGER NOT NULL DEFAULT 0"
					}
					if kind == "cache" && origin == "fork" {
						script = "ALTER TABLE nodes_dynamic ADD COLUMN penalty_units INTEGER NOT NULL DEFAULT 0; ALTER TABLE nodes_dynamic ADD COLUMN penalty_updated_at_ns INTEGER NOT NULL DEFAULT 0"
					}
					if kind == "cache" && origin == "upstream" {
						script = "ALTER TABLE nodes_dynamic ADD COLUMN chain_proxy INTEGER NOT NULL DEFAULT 0; ALTER TABLE node_latency ADD COLUMN chain_proxy INTEGER NOT NULL DEFAULT 0"
					}
					if _, err = db.Exec(script); err != nil {
						t.Fatal(err)
					}
					version := 10
					if kind == "cache" {
						version = 3
					}
					if _, err = db.Exec(fmt.Sprintf("CREATE TABLE schema_migrations (version uint64, dirty bool); INSERT INTO schema_migrations VALUES (%d, 0)", version)); err != nil {
						t.Fatal(err)
					}
				}
				migrate := MigrateStateDB
				columns := map[string][]string{"platforms": {"max_reference_latency_ms"}, "subscriptions": {"chain_proxy", "chain_relay"}}
				if kind == "cache" {
					migrate = MigrateCacheDB
					columns = map[string][]string{"nodes_dynamic": {"penalty_units", "penalty_updated_at_ns", "chain_proxy"}, "node_latency": {"chain_proxy"}}
				}
				for i := 0; i < 2; i++ {
					if err := migrate(db); err != nil {
						t.Fatal(err)
					}
				}
				for table, names := range columns {
					for _, name := range names {
						ok, err := hasTableColumn(db, table, name)
						if err != nil || !ok {
							t.Fatalf("%s.%s missing: %v", table, name, err)
						}
					}
				}
				var dirty bool
				var version int
				if err := db.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil && err != sql.ErrNoRows {
					t.Fatal(err)
				}
				if dirty || (kind == "state" && version != 11) || (kind == "cache" && version != 4) {
					t.Fatalf("version=%d dirty=%v", version, dirty)
				}
			})
		}
	}
}
