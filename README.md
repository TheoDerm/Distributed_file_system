#Description
A distributed file system with one Controller which supports the connection of N Data Stores. It supports multiple concurrent Clients sending store, load, list and remove file requests.

#Command line parameters to start up the system:

Controller: java Controller cport R timeout rebalance_period  (R = replication factor, each file is replicated R times over different data stores)

A DataStore: java Dstore port cport timeout file_folder

A Client: java Client cport timeout
