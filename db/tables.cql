CREATE TABLE metrics.records (record TEXT, time TIMESTAMP, metric TEXT, PRIMARY KEY(record, time, metric));
CREATE TABLE metrics.records_duplicates (record TEXT, metric TEXT, day TIMESTAMP, dup INT, PRIMARY KEY(record, metric));
CREATE TABLE metrics.duplicates (day TIMESTAMP, metric TEXT, dup INT, PRIMARY KEY(day, metric));
