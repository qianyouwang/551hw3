DROP TABLE IF EXISTS inode, blocks, directory;

CREATE TABLE inode (
    id INT PRIMARY KEY,
    type VARCHAR(50),
    name VARCHAR(255),
    replication INT,
    mtime BIGINT,
    atime BIGINT,
    preferredBlockSize BIGINT,
    permission VARCHAR(100)
);

CREATE TABLE blocks (
    id INT PRIMARY KEY,
    inumber INT,
    genstamp INT,
    numBytes INT,
    FOREIGN KEY (inumber) REFERENCES inode(id)
);

CREATE TABLE directory (
    parent INT,
    child INT,
    FOREIGN KEY (parent) REFERENCES inode(id),
    FOREIGN KEY (child) REFERENCES inode(id)
);