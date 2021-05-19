package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Sequence returns a pointer to a Builder for a sequence
func Sequence(name, db, schema string) *SequenceBuilder {
	return &SequenceBuilder{
		name:      name,
		db:        db,
		schema:    schema,
		increment: 1,
		start:     1,
	}
}

type sequence struct {
	Name       sql.NullString `db:"name"`
	DBName     sql.NullString `db:"database_name"`
	SchemaName sql.NullString `db:"schema_name"`
	NextValue  sql.NullString `db:"next_value"`
	Increment  sql.NullString `db:"interval"`
	CreatedOn  sql.NullString `db:"created_on"`
	Owner      sql.NullString `db:"owner"`
	Comment    sql.NullString `db:"comment"`
}

type SequenceBuilder struct {
	name      string
	db        string
	schema    string
	increment int
	comment   string
	start     int
}

// Drop returns the SQL query that will drop a sequence.
func (tb *SequenceBuilder) Drop() string {
	return fmt.Sprintf(`DROP SEQUENCE %v`, tb.QualifiedName())
}

// Drop returns the SQL query that will drop a sequence.
func (tb *SequenceBuilder) Show() string {
	return fmt.Sprintf(`SHOW SEQUENCES LIKE '%v' IN SCHEMA "%v"."%v"`, tb.name, tb.db, tb.schema)
}

func (tb *SequenceBuilder) RemoveComment() string {
	return fmt.Sprintf(`ALTER SEQUENCE %v UNSET COMMENT`, tb.QualifiedName())
}

func (tb *SequenceBuilder) ChangeComment(comment string) string {
	return fmt.Sprintf(`ALTER SEQUENCE %v SET COMMENT = '%v'`, tb.QualifiedName(), comment)
}

func (tb *SequenceBuilder) Rename(name string) string {
	return fmt.Sprintf(`ALTER SEQUENCE %v RENAME TO "%v"."%v"."%v"`, tb.QualifiedName(), tb.db, tb.schema, name)
}

func (tb *SequenceBuilder) Increment() string {
	return fmt.Sprintf(`SELECT %v.nextval`, tb.QualifiedName())
}

func (tb *SequenceBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SEQUENCE %v`, tb.QualifiedName()))
	if tb.start != 1 {
		q.WriteString(fmt.Sprintf(` START = %d`, tb.start))
	}
	if tb.increment != 1 {
		q.WriteString(fmt.Sprintf(` INCREMENT = %d`, tb.increment))
	}
	if tb.comment != "" {
		q.WriteString(fmt.Sprintf(` COMMENT = '%v'`, EscapeString(tb.comment)))
	}
	return q.String()
}

//TODO: rename tb to sb
func (tb *SequenceBuilder) WithComment(comment string) *SequenceBuilder {
	tb.comment = comment
	return tb
}

func (tb *SequenceBuilder) WithIncrement(increment int) *SequenceBuilder {
	tb.increment = increment
	return tb
}

func (tb *SequenceBuilder) WithStart(start int) *SequenceBuilder {
	tb.start = start
	return tb
}

func (tb *SequenceBuilder) QualifiedName() string {
	return fmt.Sprintf(`"%v"."%v"."%v"`, tb.db, tb.schema, tb.name)
}

func ScanSequence(row *sqlx.Row) (*sequence, error) {
	d := &sequence{}
	e := row.StructScan(d)
	return d, e
}

func ListSequences(sdb *sqlx.DB) ([]sequence, error) {
	stmt := "SHOW SEQUENCES"
	rows, err := sdb.Queryx(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []sequence{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no sequence found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
