import pandas as pd
import sys
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lxml import etree

# Define database connection parameters
DB_USER = "dsci551"
DB_PASSWORD = "Dsci-551"
DB_HOST = "localhost"
DB_NAME = "dsci551"

# Parse command line arguments
image = open(sys.argv[1])

# Connect to the database
pymysql.install_as_MySQLsb()
engine = create_engine(f"mysql+mysqldb://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
conn = engine.connect()

session = sessionmaker(bind=engine)()

# Load the fsimage file
tree = etree.parse(image)

# Extract inode information and store it in the database
inodes = tree.xpath("//inode")
inode_data = []
for inode in inodes:
    inode_id = int(inode.xpath("./id")[0].text) if inode.xpath("./id") else 0
    inode_type = inode.xpath("./type")[0].text if (inode.xpath("./type")[0].text) else ""
    inode_name = inode.xpath("./name")[0].text if (inode.xpath("./name")[0].text) else ""
    inode_replication = int(inode.xpath("./replication")[0].text) if inode.xpath("./replication") else 0
    inode_mtime = int(inode.xpath("./mtime")[0].text) if inode.xpath("./mtime") else 0
    inode_atime = int(inode.xpath("./atime")[0].text) if inode.xpath("./atime") else 0
    inode_blocksize = int(inode.xpath("./preferredBlockSize")[0].text) if inode.xpath("./preferredBlockSize") else 0
    inode_permission = inode.xpath("./permission")[0].text if (inode.xpath("./permission")[0].text) else ""
    inode_data.append((inode_id, inode_type, inode_name, inode_replication, inode_mtime, inode_atime, inode_blocksize, inode_permission))
inode_df = pd.DataFrame(inode_data, columns=["id", "type", "name", "replication", "mtime", "atime", "preferredBlockSize", "permission"])
inode_df.to_sql("inode", con=conn, if_exists="append", index=False)

# Extract directory information and store it in the database
directories = tree.xpath("//directory")
directory_data = []

for directory in directories:
    parent = int(directory.xpath("./parent")[0].text)
    children = directory.xpath("./child")
    for child in children:
        child_inumber = int(child.text)
        directory_data.append((parent, child_inumber))
        
directory_df = pd.DataFrame(directory_data, columns=["parent", "child"])
directory_df.to_sql("directory", con=conn, if_exists="append", index=False)

session.commit()

# Close the database connection
conn.close()