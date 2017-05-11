DROP KEYSPACE IF EXISTS metrics;
CREATE KEYSPACE metrics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
