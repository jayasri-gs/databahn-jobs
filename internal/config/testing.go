package config

import "gorm.io/gorm"

// SetDBForTest replaces the global database handle. Intended for unit tests only.
func SetDBForTest(connection *gorm.DB) {
	db = connection
}
