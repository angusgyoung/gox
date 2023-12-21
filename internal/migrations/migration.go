package migrations

import (
	"embed"
	"errors"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	log "github.com/sirupsen/logrus"
	"strings"

	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed changelog
var changelog embed.FS

func ApplyMigrations(databaseUrl string) error {
	// We need to replace the DSN with pgx5 to avoid
	// having to use the postgres driver
	migrationConnectionUrl := strings.Replace(
		databaseUrl,
		"postgres://",
		"pgx5://",
		1,
	)

	// Create an iofs driver to access our embedded
	// migrations fs
	migrationsDriver, err := iofs.New(changelog, "changelog")
	if err != nil {
		log.WithError(err).Warn("Failed to create migrations iofs")
		return err
	}

	m, err := migrate.NewWithSourceInstance(
		"iofs",
		migrationsDriver,
		migrationConnectionUrl,
	)
	if err != nil {
		log.WithError(err).Warn("Failed to initialise migrations")
		return err
	}

	err = m.Up()
	if err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			log.WithError(err).Warn("Failed to execute migrations")
			return err
		}
	}

	return nil
}
