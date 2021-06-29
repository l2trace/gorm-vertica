package vertica

import "gorm.io/gorm/migrator"

type Migrator struct {
	migrator.Migrator
	Dialector
}
