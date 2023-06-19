package dbfs

import (
	"embed"
	"fmt"
	"io/fs"

	"github.com/GuiaBolso/darwin"
	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func getMigrations() (migrations []darwin.Migration, ret error) {
	defer func() {
		if ret != nil {
			migrations = nil
		}
	}()

	readFile := func(fname string) []byte {
		data, err := fs.ReadFile(migrationFiles, fname)
		if err != nil {
			ret = multierror.Append(
				ret,
				fmt.Errorf("cannot read migration file %s: %w", fname, err),
			)
			return nil
		}
		return data
	}

	migrations = []darwin.Migration{
		{
			Version:     1.0,
			Description: "base database structure for a read only fs implementation",
			Script:      string(readFile("migrations/01_base_sqlite.sql")),
		},
	}

	return
}

func runMigrations(db *sqlx.DB) (ret error) {
	migrations, err := getMigrations()
	if err != nil {
		return fmt.Errorf("cannot get migrations: %w", err)
	}

	if err := darwin.Migrate(
		darwin.NewGenericDriver(db.DB, darwin.SqliteDialect{}),
		migrations,
		nil,
	); err != nil {
		return fmt.Errorf("cannot run database migrations: %w", err)
	}
	return nil
}
