--check table size
SELECT segment_name,
SUM(bytes)/1024/1024/1024 GB
FROM user_segments
WHERE segment_NAME='TA6908MOB_SAVEDOCLOG_DATA'
--and segment_name=upper('&TA6908MOB_SAVEDOCLOG_DATA')
GROUP BY segment_name;


-- check the compress is enabled or not
SELECT *
FROM   user_tables
WHERE  table_name = 'TA6908MOB_SAVEDOCLOG_DATA'

-- another check for compression
ALTER TABLE test_tab COMPRESS; -- make a new data as compressed
ALTER TABLE test_tab MOVE COMPRESS; -- make the new and old data as compress and reduce the table size