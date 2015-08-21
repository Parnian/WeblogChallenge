pv = LOAD 'file:/home/palimi/Pig/hadoop/hadoop-2.6.0/2015_07_22_mktplace_shop_web_log_sample.log' USING PigStorage('"') AS (a:chararray, b:chararray, c:chararray, d:chararray, e:chararray);

new_pv = FOREACH pv GENERATE a, REPLACE(b,' ', '_'), c, REPLACE(d,' ', '_'), e;

STORE new_pv INTO 'clean.log' using PigStorage(' ');
fs -getmerge clean.log final.log

