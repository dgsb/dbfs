package dbfs_test

import (
	_ "embed"
	"io/fs"
	"testing"

	. "github.com/dgsb/dbfs"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/le_lac.txt
var leLac string

func TestSqlite3Version(t *testing.T) {
	_, num, _ := sqlite3.Version()
	require.Equal(t, 3, num/1000_000)
	require.Equal(t, 42, (num%1000_000)/1000)
}

func Test_FSCreate(t *testing.T) {
	fs, err := NewSqliteFS(":memory:")
	require.NoError(t, err)
	require.NotNil(t, fs)
	t.Cleanup(func() {
		require.NoError(t, fs.Close())
	})
}

func Test_AddFile(t *testing.T) {
	sqlitefs, err := NewSqliteFS("/tmp/test_sqlite3fs.db")
	require.NoError(t, err)
	require.NotNil(t, sqlitefs)
	t.Cleanup(func() {
		sqlitefs.Close()
	})

	err = sqlitefs.UpsertFile("/a/regular/file", 1024, []byte(`bonjour`))
	require.NoError(t, err)

	f, err := sqlitefs.Open("/a/regular/file")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	check, err := fs.ReadFile(sqlitefs, "/a/regular/file")
	require.NoError(t, err)
	require.Equal(t, `bonjour`, string(check))

	err = sqlitefs.UpsertFile("/poésie/lamartine/le_lac", 32, []byte(leLac))
	require.NoError(t, err)
}
