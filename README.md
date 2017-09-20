# DropBin
## Overview
A backup server that accepts connections from multiple clients simultaneously, and then proceeds to synchronize file contents between the clients. Backup.py is the server side, primary.py is the client side. Created for CS 4410: Operating Systems at Cornell University.

## Usage
1. Run backup.py with two arguments \<hostname\> and \<port\> to instantiate or load a backup and leave the server running. When backup.py is killed, its synced files are pickled and saved in a database file fs.pkl
2. Run primary.py with three arguments \<hostname\> \<port\> and \<filename\> while backup is running to sync the file to the backup
