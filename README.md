

-- Load log file
logs = LOAD '/home/hadoop/logs/access_log.txt'
       USING TextLoader() AS (line:chararray);

-- Extract fields using REGEX
log_data = FOREACH logs GENERATE
    REGEX_EXTRACT(line, '^(\\S+)', 1) AS ip,                              -- IP Address
    REGEX_EXTRACT(line, '\\[(\\d+/\\w+/\\d+)', 1) AS date,               -- Date
    REGEX_EXTRACT(line, '"\\S+\\s(\\S+)\\s', 1) AS page,                 -- Requested Page
    (int)REGEX_EXTRACT(line, '"\\s(\\d{3})\\s', 1) AS status;            -- Status Code

-- Total number of hits
total_hits = FOREACH (GROUP log_data ALL) GENERATE COUNT(log_data) AS total_hits;

-- Top 5 most frequent IPs
group_by_ip = GROUP log_data BY ip;
ip_hits = FOREACH group_by_ip GENERATE group AS ip, COUNT(log_data) AS hits;
top_ips = ORDER ip_hits BY hits DESC;
top5_ips = LIMIT top_ips 5;

-- Most requested pages
group_by_page = GROUP log_data BY page;
page_hits = FOREACH group_by_page GENERATE group AS page, COUNT(log_data) AS hits;
top_pages = ORDER page_hits BY hits DESC;
top5_pages = LIMIT top_pages 5;

-- Count of 404 errors
errors = FILTER log_data BY status == 404;
error_count = FOREACH (GROUP errors ALL) GENERATE COUNT(errors) AS error_hits;

-- Hits per day
group_by_date = GROUP log_data BY date;
daily_hits = FOREACH group_by_date GENERATE group AS date, COUNT(log_data) AS hits;

-- Store results
STORE total_hits INTO '/home/hadoop/output/total_hits' USING PigStorage('\t');
STORE top5_ips INTO '/home/hadoop/output/top5_ips' USING PigStorage('\t');
STORE top5_pages INTO '/home/hadoop/output/top5_pages' USING PigStorage('\t');
STORE error_count INTO '/home/hadoop/output/error_count' USING PigStorage('\t');
STORE daily_hits INTO '/home/hadoop/output/daily_hits' USING PigStorage('\t');
