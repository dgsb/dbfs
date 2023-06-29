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
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type FS struct {
	db        *sqlx.DB
	rootInode int
	nameiStmt *sqlx.Stmt
}

var (
	InvalidPathErr   = fmt.Errorf("invalid path")
	InodeNotFoundErr = fmt.Errorf("cannot find inode")
	IncorrectTypeErr = fmt.Errorf("incorrect file type")
	DirNotEmptyErr   = fmt.Errorf("directory is not empty")
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

	fs.nameiStmt, err = fs.db.Preparex(
		"SELECT inode, type FROM github_dgsb_dbfs_files WHERE parent = ? AND fname = ?")
	if err != nil {
		return nil, fmt.Errorf("cannot prepare namei statement: %w", err)
	}

	return fs, nil
}

func (f *FS) Close() error {
	return f.db.Close()
}

func (f *FS) addRegularFileNode(tx *sqlx.Tx, fname string) (int, error) {
	components := strings.Split(fname, "/")
	var parentInode = f.rootInode
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
	if path.IsAbs(fname) {
		return fmt.Errorf("%w: %s", InvalidPathErr, fname)
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

func (fs *FS) namei(tx *sqlx.Tx, fname string) (int, string, error) {
	if path.IsAbs(fname) {
		return 0, "", fmt.Errorf("%w: %s", InvalidPathErr, fname)
	}
	if fname == "." {
		return fs.rootInode, DirectoryType, nil
	}
	components := strings.Split(fname, "/")

	var (
		inode int
		ftype string
	)
	stmt := tx.Stmtx(fs.nameiStmt)
	for i, parentInode := 0, fs.rootInode; i < len(components); i, parentInode = i+1, inode {
		row := stmt.QueryRowx(parentInode, components[i])
		if err := row.Scan(&inode, &ftype); errors.Is(err, sql.ErrNoRows) {
			return 0, "", fmt.Errorf(
				"%w: parent inode %d, fname %s", InodeNotFoundErr, parentInode, components[i])
		} else if err != nil {
			return 0, "", fmt.Errorf(
				"querying file table: inode %d, fname %s, %w", parentInode, components[i], err)
		}
	}
	return inode, ftype, nil
}

func (fsys *FS) DeleteFile(fname string) (ret error) {
	tx, err := fsys.db.Beginx()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer func() {
		if ret == nil {
			ret = tx.Commit()
		} else {
			ret = multierror.Append(ret, tx.Rollback())
		}
	}()

	inode, _, err := fsys.namei(tx, fname)
	if err != nil {
		return fmt.Errorf("cannot find inode for %s: %w", fname, err)
	}

	// Check this is not a directory tree with children
	var childCount int
	row := tx.QueryRow("SELECT count(1) FROM github_dgsb_dbfs_files WHERE parent = ?", inode)
	if err := row.Scan(&childCount); err != nil {
		return fmt.Errorf("cannot count children: %w", err)
	}
	if childCount > 0 {
		return fmt.Errorf("%w: %s", err, fname)
	}

	if _, err := tx.Exec("DELETE FROM github_dgsb_dbfs_chunks WHERE inode = ?", inode); err != nil {
		return fmt.Errorf("cannot delete file chunks: %w", err)
	}

	if _, err := tx.Exec("DELETE FROM github_dgsb_dbfs_files WHERE inode = ?", inode); err != nil {
		return fmt.Errorf("cannot delete file entry: %w", err)
	}

	return nil
}

func (fsys *FS) Open(fname string) (retFile fs.File, retError error) {

	if !fs.ValidPath(fname) {
		return nil, fmt.Errorf("path to open is invalid: %s", fname)
	}
	f := &File{db: fsys.db, name: fname}

	if strings.HasPrefix(fname, "./") {
		fname, _ = strings.CutPrefix(fname, ".")
	}
	fname = path.Clean(fname)

	tx, err := fsys.db.Beginx()
	if err != nil {
		fmt.Errorf("cannot start transaction: %w", err)
	}
	defer tx.Commit()

	inode, ftype, err := fsys.namei(tx, fname)
	if err != nil {
		return nil, fmt.Errorf("namei on %s: %w", fname, err)
	}
	f.inode = inode
	f.ftype = ftype

	row := tx.QueryRowx(
		"SELECT COALESCE(sum(size), 0) FROM github_dgsb_dbfs_chunks WHERE inode = ?", f.inode)
	if err := row.Scan(&f.size); err != nil {
		return nil, fmt.Errorf("file chunks not found: %d, %w", inode, err)
	}

	return f, nil
}

type File struct {
	db     *sqlx.DB
	ftype  string
	name   string
	inode  int
	offset int64
	size   int64
	closed bool
	eof    bool
}

func (f *File) Read(out []byte) (int, error) {
	if f.ftype != RegularFileType {
		return 0, fmt.Errorf("%w: %s", IncorrectTypeErr, f.ftype)
	}
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
		FROM github_dgsb_dbfs_chunks JOIN offsets USING (position)
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
	return FileInfo{
		name:  f.name,
		size:  f.size,
		ftype: f.ftype,
	}, nil
}

func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.ftype != DirectoryType {
		return []fs.DirEntry{}, fmt.Errorf("%w: %s", IncorrectTypeErr, f.ftype)
	}
	if f.eof {
		if n > 0 {
			return []fs.DirEntry{}, io.EOF
		}
		return []fs.DirEntry{}, nil
	}
	query := `
		SELECT
			github_dgsb_dbfs_files.inode,
			fname,
			type,
			SUM(COALESCE(size, 0)) size
		FROM github_dgsb_dbfs_files LEFT JOIN github_dgsb_dbfs_chunks USING (inode)
		WHERE parent = ? AND inode > ?
		GROUP BY github_dgsb_dbfs_files.inode, fname, type
		ORDER BY inode`
	if n > 0 {
		query += fmt.Sprintf(` LIMIT %d`, n)
	}
	rows, err := f.db.Queryx(query, f.inode, f.offset)
	if err != nil {
		return []fs.DirEntry{}, fmt.Errorf("cannot not query file table: %w", err)
	}
	defer rows.Close()

	files := []File{}
	for rows.Next() {
		var entry File

		if err := rows.Scan(&entry.inode, &entry.name, &entry.ftype, &entry.size); err != nil {
			return []fs.DirEntry{}, fmt.Errorf("cannot scan database row: %w", err)
		}
		files = append(files, entry)
		f.offset = int64(entry.inode)
	}
	if err := rows.Err(); err != nil {
		return []fs.DirEntry{}, fmt.Errorf("cannot browse file table: %w", err)
	}

	entries := make([]fs.DirEntry, 0, len(files))
	for _, v := range files {
		if fi, err := v.Stat(); err != nil {
			return []fs.DirEntry{}, fmt.Errorf("cannot stat file with inode %d: %w", v.inode, err)
		} else {
			entries = append(entries, fs.FileInfoToDirEntry(fi))
		}
	}

	if len(entries) == 0 {
		f.eof = true
	}

	return entries, nil
}

type FileInfo struct {
	name  string
	ftype string
	size  int64
}

func (fi FileInfo) Name() string {
	return path.Base(fi.name)
}

func (fi FileInfo) Size() int64 {
	return fi.size
}

func (fi FileInfo) Mode() fs.FileMode {
	if fi.ftype == DirectoryType {
		return 0444 | fs.ModeDir
	}

	return 0444
}

func (fi FileInfo) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (fi FileInfo) IsDir() bool {
	return fi.ftype == DirectoryType
}

func (fi FileInfo) Sys() any {
	return nil
}
