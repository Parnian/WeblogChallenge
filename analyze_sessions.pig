pv = LOAD 'file:/home/palimi/Pig/hadoop/hadoop-2.6.0/final.log' USING PigStorage(' ') AS (timestamp:datetime, elb:chararray, client_port:chararray, backend_port:chararray, request_processing_time:float, backend_processing_time:float, response_processing_time:float, elb_status_code:int, backend_status_code:int, received_bytes:int, sent_bytes:int, chararray, request:chararray, chararray, user_agent:chararray, chararray, ssl_cipher:chararray, ssl_protocol:chararray);

register datafu-1.2.0.jar

DEFINE Sessionize datafu.pig.sessions.Sessionize('$TIME_WINDOW');

views = FOREACH pv GENERATE ToUnixTime(timestamp) AS timestamp:long, client_port;

views = GROUP views BY client_port;

sessions = FOREACH views {
   visits = ORDER views BY timestamp;
   GENERATE FLATTEN(Sessionize(visits)) AS (timestamp, client_port, session_id); 
 };


session_times =
  FOREACH (GROUP sessions BY (session_id,client_port)) {
    GENERATE group.session_id as session_id,
             group.client_port as client_port,
             (MAX(sessions.timestamp) - MIN(sessions.timestamp))
               / 1000.0 / 60.0 as session_length;
};

DEFINE Median datafu.pig.stats.StreamingMedian();
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.9','0.95');

session_stats = FOREACH (GROUP session_times ALL) {
  GENERATE
    AVG(session_times.session_length) as avg_session,
    Median(session_times.session_length) as median_session,
    Quantile(session_times.session_length) as quantile_session;
};


long_sessions = FILTER session_times BY
  session_length > session_stats.quantile_session.quantile_0_95;

very_engaged_users = DISTINCT (FOREACH long_sessions GENERATE client_port);

STORE very_engaged_users INTO 'very_engaged_users_$TIME_WINDOW.log';

fs -getmerge very_engaged_users_$TIME_WINDOW.log top_users.log;

