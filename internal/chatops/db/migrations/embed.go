// Package migrations embeds the goose SQL migrations applied in-process at
// startup.
package migrations

import "embed"

// FS holds the embedded goose migration files.
//
//go:embed *.sql
var FS embed.FS
