package risingwave

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"github.com/docker/go-connections/nat"
	"io"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/dhui/dktest"

	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	pgPassword = "postgres"
)

var (
	opts = dktest.Options{
		Env:          map[string]string{"POSTGRES_PASSWORD": pgPassword},
		PortRequired: true,
		ReadyFunc:    isReady,
		ExposedPorts: map[nat.Port]struct{}{"4566/tcp": {}}}
	specs = []dktesting.ContainerSpec{
		// a bug GRANT USAGE ON SCHEMA is fixed on 1.4 (unreleased yet)
		{ImageName: "risingwavelabs/risingwave:nightly-20231101", Options: opts},
	}
)

func pgConnectionString(host, port string, options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf("postgres://postgres:%s@%s:%s/dev?%s", pgPassword, host, port, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(4566)
	if err != nil {
		return false
	}

	db, err := sql.Open("postgres", pgConnectionString(ip, port))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func mustRun(t *testing.T, d database.Driver, statements []string) {
	for _, statement := range statements {
		if err := d.Run(strings.NewReader(statement)); err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "dev", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatements(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists bool
		if err := d.(*RisingWave).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func TestErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		//wantErr := `migration failed: syntax error at or near "TABLEE" (column 37) in line 1: CREATE TABLE foo ` +
		//	`(foo text); CREATE TABLEE bar (bar text); (details: pq: syntax error at or near "TABLEE")`
		wantErr := `sql parser error: Expected an object type after CREATE, found: TABLEE`
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLEE bar (bar text);")); err == nil {
			t.Fatal("expected err but got nil")
		} else if !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&x-custom=foobar",
			pgPassword, ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func TestWithSchema(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// create foobar schema
		if err := d.Run(strings.NewReader("CREATE SCHEMA foobar")); err != nil {
			t.Fatal(err)
		}
		if err := d.SetVersion(1, false); err != nil {
			t.Fatal(err)
		}

		// re-connect using that schema
		d2, err := p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&search_path=foobar",
			pgPassword, ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d2.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		version, _, err := d2.Version()
		if err != nil {
			t.Fatal(err)
		}
		// FIXME: This failed because RisingWave doesn't handle start-up options (search_path here) correctly
		// https://github.com/risingwavelabs/risingwave/issues/13210
		if version != database.NilVersion {
			t.Fatal("expected NilVersion, got:", version)
		}

		// now update version and compare
		if err := d2.SetVersion(2, false); err != nil {
			t.Fatal(err)
		}
		version, _, err = d2.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatal("expected version 2")
		}

		// meanwhile, the public schema still has the other version
		version, _, err = d.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatal("expected version 2")
		}
	})
}

func TestMigrationTableOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, _ := p.Open(addr)
		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// create migrate schema
		if err := d.Run(strings.NewReader("CREATE SCHEMA migrate ")); err != nil {
			t.Fatal(err)
		}

		// bad unquoted x-migrations-table parameter
		wantErr := "x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: migrate.schema_migrations"
		d, err = p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&x-migrations-table=migrate.schema_migrations&x-migrations-table-quoted=1",
			pgPassword, ip, port))
		if (err != nil) && (err.Error() != wantErr) {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}

		// too many quoted x-migrations-table parameters
		wantErr = "\"\"migrate\".\"schema_migrations\".\"toomany\"\" MigrationsTable contains too many dot characters"
		d, err = p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&x-migrations-table=\"migrate\".\"schema_migrations\".\"toomany\"&x-migrations-table-quoted=1",
			pgPassword, ip, port))
		if (err != nil) && (err.Error() != wantErr) {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}

		// good quoted x-migrations-table parameter
		d, err = p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&x-migrations-table=\"migrate\".\"schema_migrations\"&x-migrations-table-quoted=1",
			pgPassword, ip, port))
		if err != nil {
			t.Fatal(err)
		}

		// make sure migrate.schema_migrations table exists
		var exists bool
		if err := d.(*RisingWave).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'schema_migrations' AND table_schema = 'migrate')").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table migrate.schema_migrations to exist")
		}

		d, err = p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&x-migrations-table=migrate.schema_migrations",
			pgPassword, ip, port))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.(*RisingWave).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'migrate.schema_migrations' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table 'migrate.schema_migrations' to exist")
		}

	})
}

// FIXME: This test fails because RisingWave doesn't support REVOKE FROM PUBLIC yet.
func TestFailToCreateTableWithoutPermissions(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)

		// Check that opening the postgres connection returns NilVersion
		p := &RisingWave{}

		d, err := p.Open(addr)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create user who is not the owner. Although we're concatenating strings in an sql statement it should be fine
		// since this is a test environment and we're not expecting to the pgPassword to be malicious
		mustRun(t, d, []string{
			"CREATE USER not_owner WITH ENCRYPTED PASSWORD '" + pgPassword + "'",
			"CREATE SCHEMA barfoo ",
			"GRANT USAGE ON SCHEMA barfoo TO not_owner",
			"REVOKE CREATE ON SCHEMA barfoo FROM PUBLIC",
			"REVOKE CREATE ON SCHEMA barfoo FROM not_owner",
		})

		// re-connect using that schema
		d2, err := p.Open(fmt.Sprintf("postgres://not_owner:%s@%v:%v/dev?sslmode=disable&search_path=barfoo",
			pgPassword, ip, port))

		defer func() {
			if d2 == nil {
				return
			}
			if err := d2.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		var e *database.Error
		if !errors.As(err, &e) || err == nil {
			t.Fatal("Unexpected error, want permission denied error. Got: ", err)
		}

		if !strings.Contains(e.OrigErr.Error(), "permission denied for schema barfoo") {
			t.Fatal(e)
		}

		// re-connect using that x-migrations-table and x-migrations-table-quoted
		d2, err = p.Open(fmt.Sprintf("postgres://not_owner:%s@%v:%v/dev?sslmode=disable&x-migrations-table=\"barfoo\".\"schema_migrations\"&x-migrations-table-quoted=1",
			pgPassword, ip, port))

		if !errors.As(err, &e) || err == nil {
			t.Fatal("Unexpected error, want permission denied error. Got: ", err)
		}

		if !strings.Contains(e.OrigErr.Error(), "permission denied for schema barfoo") {
			t.Fatal(e)
		}
	})
}

// FIXME: This test fails because RisingWave doesn't support REVOKE FROM PUBLIC yet.
func TestCheckBeforeCreateTable(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)

		// Check that opening the postgres connection returns NilVersion
		p := &RisingWave{}

		d, err := p.Open(addr)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create user who is not the owner. Although we're concatenating strings in an sql statement it should be fine
		// since this is a test environment and we're not expecting to the pgPassword to be malicious
		mustRun(t, d, []string{
			"CREATE USER not_owner WITH ENCRYPTED PASSWORD '" + pgPassword + "'",
			"CREATE SCHEMA barfoo ",
			"GRANT USAGE ON SCHEMA barfoo TO not_owner",
			"GRANT CREATE ON SCHEMA barfoo TO not_owner",
		})

		// re-connect using that schema
		d2, err := p.Open(fmt.Sprintf("postgres://not_owner:%s@%v:%v/dev?sslmode=disable&search_path=barfoo",
			pgPassword, ip, port))

		if err != nil {
			t.Fatal(err)
		}

		if err := d2.Close(); err != nil {
			t.Fatal(err)
		}

		// revoke privileges
		mustRun(t, d, []string{
			"REVOKE CREATE ON SCHEMA barfoo FROM PUBLIC",
			"REVOKE CREATE ON SCHEMA barfoo FROM not_owner",
		})

		// re-connect using that schema
		d3, err := p.Open(fmt.Sprintf("postgres://not_owner:%s@%v:%v/dev?sslmode=disable&search_path=barfoo",
			pgPassword, ip, port))

		if err != nil {
			t.Fatal(err)
		}

		version, _, err := d3.Version()

		if err != nil {
			t.Fatal(err)
		}

		if version != database.NilVersion {
			t.Fatal("Unexpected version, want database.NilVersion. Got: ", version)
		}

		defer func() {
			if err := d3.Close(); err != nil {
				t.Fatal(err)
			}
		}()
	})
}

func TestParallelSchema(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create foo and bar schemas
		if err := d.Run(strings.NewReader("CREATE SCHEMA foo ")); err != nil {
			t.Fatal(err)
		}
		if err := d.Run(strings.NewReader("CREATE SCHEMA bar ")); err != nil {
			t.Fatal(err)
		}

		// re-connect using that schemas
		dfoo, err := p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&search_path=foo",
			pgPassword, ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := dfoo.Close(); err != nil {
				t.Error(err)
			}
		}()

		dbar, err := p.Open(fmt.Sprintf("postgres://postgres:%s@%v:%v/dev?sslmode=disable&search_path=bar",
			pgPassword, ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := dbar.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := dfoo.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Unlock(); err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRisingWave_Lock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &RisingWave{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		dt.Test(t, d, []byte("SELECT 1"))

		ps := d.(*RisingWave)

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

// FIXME: This test failed because RisingWave doesn't handle concurrent DDL well. https://github.com/risingwavelabs/risingwave/issues/10289
func TestWithInstance_Concurrent(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		// The number of concurrent processes running WithInstance
		const concurrency = 30

		// We can instantiate a single database handle because it is
		// actually a connection pool, and so, each of the below go
		// routines will have a high probability of using a separate
		// connection, which is something we want to exercise.
		db, err := sql.Open("postgres", pgConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		db.SetMaxIdleConns(concurrency)
		db.SetMaxOpenConns(concurrency)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				defer wg.Done()
				_, err := WithInstance(db, &Config{})
				if err != nil {
					t.Errorf("process %d error: %s", i, err)
				}
			}(i)
		}
	})
}

func TestWithConnection(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(4566)
		if err != nil {
			t.Fatal(err)
		}

		db, err := sql.Open("postgres", pgConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		ctx := context.Background()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}

		p, err := WithConnection(ctx, conn, &Config{})
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := p.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, p, []byte("SELECT 1"))
	})
}

func Test_computeLineFromPos(t *testing.T) {
	testcases := []struct {
		pos      int
		wantLine uint
		wantCol  uint
		input    string
		wantOk   bool
	}{
		{
			15, 2, 6, "SELECT *\nFROM foo", true, // foo table does not exists
		},
		{
			16, 3, 6, "SELECT *\n\nFROM foo", true, // foo table does not exists, empty line
		},
		{
			25, 3, 7, "SELECT *\nFROM foo\nWHERE x", true, // x column error
		},
		{
			27, 5, 7, "SELECT *\n\nFROM foo\n\nWHERE x", true, // x column error, empty lines
		},
		{
			10, 2, 1, "SELECT *\nFROMM foo", true, // FROMM typo
		},
		{
			11, 3, 1, "SELECT *\n\nFROMM foo", true, // FROMM typo, empty line
		},
		{
			17, 2, 8, "SELECT *\nFROM foo", true, // last character
		},
		{
			18, 0, 0, "SELECT *\nFROM foo", false, // invalid position
		},
	}
	for i, tc := range testcases {
		t.Run("tc"+strconv.Itoa(i), func(t *testing.T) {
			run := func(crlf bool, nonASCII bool) {
				var name string
				if crlf {
					name = "crlf"
				} else {
					name = "lf"
				}
				if nonASCII {
					name += "-nonascii"
				} else {
					name += "-ascii"
				}
				t.Run(name, func(t *testing.T) {
					input := tc.input
					if crlf {
						input = strings.Replace(input, "\n", "\r\n", -1)
					}
					if nonASCII {
						input = strings.Replace(input, "FROM", "FRÖM", -1)
					}
					gotLine, gotCol, gotOK := computeLineFromPos(input, tc.pos)

					if tc.wantOk {
						t.Logf("pos %d, want %d:%d, %#v", tc.pos, tc.wantLine, tc.wantCol, input)
					}

					if gotOK != tc.wantOk {
						t.Fatalf("expected ok %v but got %v", tc.wantOk, gotOK)
					}
					if gotLine != tc.wantLine {
						t.Fatalf("expected line %d but got %d", tc.wantLine, gotLine)
					}
					if gotCol != tc.wantCol {
						t.Fatalf("expected col %d but got %d", tc.wantCol, gotCol)
					}
				})
			}
			run(false, false)
			run(true, false)
			run(false, true)
			run(true, true)
		})
	}
}
