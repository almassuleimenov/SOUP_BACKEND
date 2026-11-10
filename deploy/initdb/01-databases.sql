-- Separate databases so the chaos harness (which TRUNCATEs) and the tests
-- never touch the data of the normally running service.
CREATE DATABASE jobq_chaos OWNER jobq;
CREATE DATABASE jobq_test OWNER jobq;
