// Package dbfs implement the fs.FS over a sqlite3 database backend.
package dbfs

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type FS struct {
	db        *sqlx.DB
	rootInode int
}

var (
	RelativePathErr  = fmt.Errorf("relative path are not supported")
	InodeNotFoundErr = fmt.Errorf("cannot find inode")
	IncorrectTypeErr = fmt.Errorf("incorrect file type")
)

const (
	DirectoryType   = "d"
	RegularFileType = "f"
)

func NewSqliteFS(dbName string) (*FS, error) {
	db, err := sqlx.Open("sqlite3", dbName)
	if err != nil {
		return nil, fmt.Errorf("canot open the database: %w", err)
	}
	err = runMigrations(db)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_key = ON"); err != nil {
		return nil, fmt.Errorf("cannot activate foreign keys check: %w", err)
	}

	fs := &FS{db: db}
	row := db.QueryRow(`
		SELECT inode
		FROM github_dgsb_dbfs_files
		WHERE fname = '/' AND parent IS NULL`)
	if err := row.Scan(&fs.rootInode); err != nil {
		return nil, fmt.Errorf("no root inode: %w %w", InodeNotFoundErr, err)
	}

	return fs, nil
}

func (fs *FS) Close() error {
	return fs.db.Close()
}

func (fs *FS) addRegularFileNode(tx *sqlx.Tx, fname string) (int, error) {
	if !path.IsAbs(fname) {
		return 0, fmt.Errorf("%w: %s", RelativePathErr, fname)
	}

	components := strings.Split(fname, "/")[1:]
	var parentInode = fs.rootInode
	for i, searchMode := 0, true; i < len(components); i++ {
		if searchMode {
			var (
				inode int
				ftype string
			)
			row := tx.QueryRowx(
				"SELECT inode, type FROM github_dgsb_dbfs_files WHERE fname = ? AND parent = ?",
				components[i], parentInode)
			err := row.Scan(&inode, &ftype)
			if err == nil {
				parentInode = inode
				if (i < len(components)-1 && ftype != DirectoryType) ||
					(i == len(components)-1 && ftype != RegularFileType) {
					return 0, fmt.Errorf(
						"%w: %s %s", IncorrectTypeErr, "/"+strings.Join(components[:i+1], "/"), ftype)
				}
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, fmt.Errorf("cannot query files table: %w", err)
			}
			searchMode = false
		}

		componentType := func() string {
			if i < len(components)-1 {
				return DirectoryType
			}
			return RegularFileType
		}()
		row := tx.QueryRow(`
			INSERT INTO github_dgsb_dbfs_files (fname, parent, type)
			VALUES (?, ?, ?)
			RETURNING inode`, components[i], parentInode, componentType)
		if err := row.Scan(&parentInode); err != nil {
			return 0, fmt.Errorf(
				"cannot insert node %s as child of %d: %w", components[i], parentInode, err)
		}
	}

	return parentInode, nil
}

func (fs *FS) UpsertFile(fname string, chunkSize int, data []byte) (ret error) {
	if !path.IsAbs(fname) {
		return fmt.Errorf("%w: %s", RelativePathErr, fname)
	}
	fname = path.Clean(fname)

	tx, err := fs.db.Beginx()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer func() {
		if err != nil {
			ret = multierror.Append(ret, tx.Rollback())
		} else {
			ret = tx.Commit()
		}
	}()

	inode, err := fs.addRegularFileNode(tx, fname)
	if err != nil {
		return fmt.Errorf("cannot insert file node: %w", err)
	}

	if _, err := tx.Exec(`DELETE FROM github_dgsb_dbfs_chunks WHERE inode = ?`, inode); err != nil {
		return fmt.Errorf("cannot delete previous chunks of the same file %s: %w", fname, err)
	}

	for i, position := 0, 0; i < len(data); i, position = i+chunkSize, position+1 {
		toWrite := func() int {
			remaining := len(data) - i
			if remaining < chunkSize {
				return remaining
			}
			return chunkSize
		}()
		_, err := tx.Exec(`
			INSERT INTO github_dgsb_dbfs_chunks (inode, position, data, size)
			VALUES (?, ?, ?, ?)`, inode, position, data[i:i+toWrite], toWrite)
		if err != nil {
			return fmt.Errorf("cannot insert file chunk in database: %w", err)
		}
	}
	return nil
}

func (fs *FS) namei(fname string) (int, error) {
	if !path.IsAbs(fname) {
		return 0, fmt.Errorf("%w: %s", RelativePathErr, fname)
	}
	components := strings.Split(fname, "/")[1:]
	if len(components) == 0 {
		return fs.rootInode, nil
	}

	var inode int
	for i, parentInode := 0, fs.rootInode; i < len(components); i, parentInode = i+1, inode {
		row := fs.db.QueryRow(
			"SELECT inode FROM github_dgsb_dbfs_files WHERE parent = ? AND fname = ?",
			parentInode, components[i])
		if err := row.Scan(&inode); errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf(
				"%w: parent inode %d, fname %s", InodeNotFoundErr, parentInode, components[i])
		} else if err != nil {
			return 0, fmt.Errorf(
				"querying file table: inode %d, fname %s, %w", parentInode, components[i], err)
		}
	}
	return inode, nil
}

func (fs *FS) Open(fname string) (fs.File, error) {
	if !path.IsAbs(fname) {
		return nil, fmt.Errorf("relative path are not supported")
	}

	f := &File{db: fs.db}
	inode, err := fs.namei(fname)
	if err != nil {
		return nil, fmt.Errorf("namei on %s: %w", fname, err)
	}
	f.inode = inode

	row := f.db.QueryRowx(
		"SELECT COALESCE(sum(size), 0) FROM github_dgsb_dbfs_chunks WHERE inode = ?", f.inode)
	if err := row.Scan(&f.size); err != nil {
		return nil, fmt.Errorf("file chunks not found: %d, %w", inode, err)
	}

	return f, nil
}

type File struct {
	db     *sqlx.DB
	inode  int
	offset int64
	size   int64
	closed bool
}

func (f *File) Read(out []byte) (int, error) {
	if f.closed {
		return 0, fmt.Errorf("file closed")
	}
	if f.offset >= f.size {
		return 0, io.EOF
	}
	toRead := func(a, b int64) int64 {
		if a < b {
			return a
		}
		return b
	}(f.size-f.offset, int64(len(out)))

	rows, err := f.db.NamedQuery(`
		WITH offsets AS (
			SELECT
				COALESCE(
					SUM(size) OVER (
						ORDER BY POSITION ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
					),
					0
				) AS start,
				position
			FROM github_dgsb_dbfs_chunks
			WHERE inode = :inode
		)
		SELECT
			github_dgsb_dbfs_chunks.position,
			data,
			size,
			start
		FROM github_dgsb_dbfs_chunks JOIN offsets
		WHERE inode = :inode
			AND :offset < start + size
			AND :offset + :size >= start
		ORDER BY github_dgsb_dbfs_chunks.position
		`, map[string]interface{}{"inode": f.inode, "offset": f.offset, "size": toRead})
	if err != nil {
		return 0, fmt.Errorf("cannot query the database: %w", err)
	}
	defer rows.Close()

	copied := int64(0)
	for rows.Next() {
		var (
			position int
			buf      []byte
			size     int64
			offset   int64
		)
		if err := rows.Scan(&position, &buf, &size, &offset); err != nil {
			return 0, fmt.Errorf("cannot retrieve file chunk: %w", err)
		}

		numByte := int64(copy(out[copied:], buf[f.offset-offset:]))
		copied += numByte
		f.offset += numByte
		if copied >= toRead {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("cannot iterate over file chunks: %w", err)
	}

	return int(toRead), nil
}

func (f *File) Close() error {
	f.db = nil
	f.closed = true
	return nil
}

func (f *File) Stat() (fs.FileInfo, error) {
	fmt.Println("calling stat")
	defer fmt.Println("calling stat: return")
	return nil, fmt.Errorf("not yet implemented")
}