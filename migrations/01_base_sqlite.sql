CREATE TABLE github_dgsb_dbfs_files (
    inode INTEGER PRIMARY KEY AUTOINCREMENT,
    fname TEXT,
    parent INTEGER,
    type TEXT,
    FOREIGN KEY (parent) REFERENCES github_dgsb_dbfs_files(inode)
);

CREATE UNIQUE INDEX github_dgsb_dbfs_files_parent_fname ON github_dgsb_dbfs_files(parent, fname);

CREATE TABLE github_dgsb_dbfs_chunks (
    inode INTEGER,
    position INTEGER,
    data BLOB,
    size INTEGER,
    PRIMARY KEY(inode, position),
    FOREIGN KEY(inode) REFERENCES github_dgsb_dbfs_files(inode)
);

INSERT INTO github_dgsb_dbfs_files (fname, type) VALUES ('/', 'd');
