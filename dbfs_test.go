package dbfs_test

import (
	_ "embed"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"testing/quick"

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
	sqlitefs, err := NewSqliteFS(":memory:")
	require.NoError(t, err)
	require.NotNil(t, sqlitefs)
	t.Cleanup(func() {
		sqlitefs.Close()
	})

	err = sqlitefs.UpsertFile("a/regular/file", 1024, []byte(`bonjour`))
	require.NoError(t, err)

	f, err := sqlitefs.Open("a/regular/file")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	check, err := fs.ReadFile(sqlitefs, "a/regular/file")
	require.NoError(t, err)
	require.Equal(t, `bonjour`, string(check))

	err = sqlitefs.UpsertFile("poésie/lamartine/le_lac", 32, []byte(leLac))
	require.NoError(t, err)

	check, err = fs.ReadFile(sqlitefs, "poésie/lamartine/le_lac")
	require.NoError(t, err)
	require.Equal(t, leLac, string(check))

	require.NoError(t, fstest.TestFS(sqlitefs, "poésie/lamartine/le_lac", "a/regular/file"))
}

func Test_DeleteFile(t *testing.T) {
	sqliteFS, err := NewSqliteFS(":memory:")
	require.NoError(t, err)
	require.NotNil(t, sqliteFS)

	err = sqliteFS.UpsertFile("a/test/file/to/delete/later", 2, []byte("just something"))
	require.NoError(t, err)

	err = sqliteFS.UpsertFile("another/which/will/not/be/deleted", 3, []byte("somt kind of other data"))
	require.NoError(t, err)

	err = sqliteFS.DeleteFile("a/test/file/to/delete/later")
	require.NoError(t, err)

	err = fstest.TestFS(sqliteFS, "another/which/will/not/be/deleted")
	require.NoError(t, err)
}

func TestCompliance_EmptyFS(t *testing.T) {
	sqlitefs, err := NewSqliteFS(":memory:")
	require.NoError(t, err)
	require.NotNil(t, sqlitefs)
	t.Cleanup(func() {
		sqlitefs.Close()
	})

	require.NoError(t, fstest.TestFS(sqlitefs))
}

func generatePath(rng *rand.Rand) string {
	lexem := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123459"
	components := make([]string, (rng.Int()%17)+16)
	for i := 0; i < len(components); i++ {
		builder := &strings.Builder{}
		csize := rng.Int()%9 + 8
		for j := 0; j < csize; j++ {
			builder.WriteByte(lexem[rng.Int()%len(lexem)])
		}
		components[i] = builder.String()
	}
	return path.Join(components...)
}

func TestQuick(t *testing.T) {

	t.Run("simple insert", func(t *testing.T) {
		sqliteFS, err := NewSqliteFS(path.Join(t.TempDir(), "sqlitefs.db"))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, sqliteFS.Close())
		})

		quick.Check(func(path string, chunkSize int, data []byte) bool {
			require.NoError(t, sqliteFS.UpsertFile(path, chunkSize, data))
			check, err := fs.ReadFile(sqliteFS, path)
			require.NoError(t, err)
			require.Equal(t, data, check)
			return true
		}, &quick.Config{
			Values: func(values []reflect.Value, rng *rand.Rand) {
				values[0] = reflect.ValueOf(generatePath(rng))
				values[1] = reflect.ValueOf(rng.Int()%4096 + 1)
				values[2] = reflect.ValueOf(make([]byte, rng.Int()%8192+1))
			},
		})
	})

	t.Run("insert delete update", func(t *testing.T) {
		const (
			insertOp = iota
			deleteOp
			updateOp
			checkOp
		)
		operations := []int{insertOp, deleteOp, updateOp, checkOp}
		rate := []float32{0.475, 0.7125, 0.95, 1.0}
		existingFilesMap := map[string][]byte{}
		existingFilesArray := []string{}

		type Operation struct {
			Operation int
			Path      string
			Data      []byte
		}

		fsys, err := NewSqliteFS(path.Join(t.TempDir(), "quickfstest.db"))
		require.NoError(t, err)

		var generator func([]reflect.Value, *rand.Rand)

		generator = func(values []reflect.Value, rng *rand.Rand) {
			var o Operation
			for i, stat := 0, rng.Float32(); i <= len(operations); i++ {
				if stat <= rate[i] {
					o.Operation = operations[i]
					break
				}
			}

			if (o.Operation == deleteOp || o.Operation == updateOp) && len(existingFilesArray) == 0 {
				// recurse and return
				generator(values, rng)
				return
			}

			switch o.Operation {
			case insertOp:
				for {
					o.Path = generatePath(rng)
					if _, ok := existingFilesMap[o.Path]; !ok {
						break
					}
				}
				buf := make([]byte, rng.Int()%8192+1)
				rng.Read(buf)
				o.Data = buf
				existingFilesArray = append(existingFilesArray, o.Path)
				existingFilesMap[o.Path] = o.Data
			case updateOp:
				idx := rng.Int() % len(existingFilesArray)
				buf := make([]byte, rng.Int()%8192+1)
				rng.Read(buf)
				o.Path = existingFilesArray[idx]
				o.Data = buf
				existingFilesMap[o.Path] = o.Data
			case deleteOp:
				idx := rng.Int() % len(existingFilesArray)
				o.Path = existingFilesArray[idx]

				delete(existingFilesMap, existingFilesArray[idx])
				existingFilesArray[idx] = existingFilesArray[len(existingFilesArray)-1]
				existingFilesArray = existingFilesArray[:len(existingFilesArray)-1]
			}

			values[0] = reflect.ValueOf(o)
		}

		iteration := 0
		quick.Check(func(o Operation) bool {
			t.Log(iteration, "num files", len(existingFilesArray), o.Operation)
			iteration++
			switch o.Operation {
			case insertOp, updateOp:
				require.NoError(t, fsys.UpsertFile(o.Path, 32, o.Data))
			case deleteOp:
				require.NoError(t, fsys.DeleteFile(o.Path))
			case checkOp:
				require.NoError(t, fstest.TestFS(fsys, existingFilesArray...))
				for k, v := range existingFilesMap {
					data, err := fs.ReadFile(fsys, k)
					require.NoError(t, err)
					require.Equal(t, v, data)
				}
			}
			return true
		}, &quick.Config{
			MaxCount: 200,
			Values:   generator,
		})
		require.NoError(t, fstest.TestFS(fsys, existingFilesArray...))
		for k, v := range existingFilesMap {
			data, err := fs.ReadFile(fsys, k)
			require.NoError(t, err)
			require.Equal(t, v, data)
		}
	})
}

func generateData(b *testing.B) ([]string, map[string][]byte) {
	files := map[string][]byte{}
	fileList := []string{}
	rng := rand.New(rand.NewSource(28021976))
	for i := 0; i < 2000; i++ {
		var newFile string
		for {
			newFile = generatePath(rng)
			if _, ok := files[newFile]; !ok { // ensure all generated path are unique
				break
			}
		}
		buf := make([]byte, rng.Int()%8192+1)
		rng.Read(buf)
		files[newFile] = buf
		fileList = append(fileList, newFile)
	}
	return fileList, files
}

func BenchmarkSqliteFS(b *testing.B) {
	sqliteFS, err := NewSqliteFS(path.Join(b.TempDir(), "fsbench.db"))
	require.NoError(b, err)

	fileList, files := generateDate(b)
	require.NoError(b, sqliteFS.UpsertFiles(files, 8192))
	b.ResetTimer()

	b.Run("sqliteFS open close", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, fname := range fileList {
				f, err := sqliteFS.Open(fname)
				require.NoError(b, err)
				f.Close()
			}
		}
	})

	b.Run("sqliteFS", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for k, v := range files {
				data, err := fs.ReadFile(sqliteFS, k)
				require.NoError(b, err, "cannot read file %s", k)
				require.Equal(b, v, data)
			}
		}
	})
}

func BenchmarkFS(b *testing.B) {
	dirfsRoot := path.Join(b.TempDir(), "dirfsbenc")
	require.NoError(b, os.MkdirAll(dirfsRoot, 0755))
	dirFS := os.DirFS(dirfsRoot)

	files := map[string][]byte{}
	fileList := []string{}
	rng := rand.New(rand.NewSource(28021976))
	for i := 0; i < 2000; i++ {
		var newFile string
		for {
			newFile = generatePath(rng)
			if _, ok := files[newFile]; !ok { // ensure all generated path are unique
				break
			}
		}
		buf := make([]byte, rng.Int()%8192+1)
		rng.Read(buf)
		files[newFile] = buf
		fileList = append(fileList, newFile)
		require.NoError(b, os.MkdirAll(path.Join(dirfsRoot, path.Dir(newFile)), 0755))
		require.NoError(b, os.WriteFile(path.Join(dirfsRoot, newFile), buf, 0644))
	}
	require.NoError(b, sqliteFS.UpsertFiles(files, 8192))
	b.ResetTimer()

	b.Run("dirFS open close", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, fname := range fileList {
				f, err := dirFS.Open(fname)
				require.NoError(b, err)
				f.Close()
			}
		}
	})

	b.Run("dirFS", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for k, v := range files {
				data, err := fs.ReadFile(dirFS, k)
				require.NoError(b, err, "cannot read file %s", k)
				require.Equal(b, v, data)
			}
		}
	})
}
