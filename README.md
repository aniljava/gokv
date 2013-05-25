## IMPORTANT

This is an incomplete project, set up to switch between my desktop and laptop. This message will be removed once the code is minimally usable.

## Goal
Create a simple Key Value data store backed by a file. Based on configuration, can either be cached or inmemory system.

## NOTES

- MMAAP is not used, all reads and writes using plain rear/write
- Lock system is not implemented


## Notes on data HashTable

### How keys are stored.
A top level structure of the database file
	[METADATA][FIXED LENGTH KEYSPACE][BLOCKS]

	An inmemory copy of keyspace with same size is kept. 


