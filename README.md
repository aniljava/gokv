## Goal
Create a simple Key Value data store backed by a file. Based on configuration, can either be cached or inmemory system.

# Soft ACID

Soft in a sense that it ACID is not top most priority for this version.
Atomicity, Consistency, Isolation, Durability



## Notes on data HashTable

### How keys are stored.
A top level structure of the database file
	[METADATA][FIXED LENGTH KEYSPACE][BLOCKS]

	An inmemory copy of keyspace with same size is kept. 


