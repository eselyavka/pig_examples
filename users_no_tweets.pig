/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*4b*/
tweets = LOAD 'pig_examples/tweets.csv' USING PigStorage(',') AS (id:long, payload:chararray, login:chararray);
users = LOAD 'pig_examples/users.csv' USING PigStorage(',') AS (ulogin:chararray, full_name:chararray, state:chararray);
joined = FOREACH (JOIN tweets BY login RIGHT OUTER, users BY ulogin) GENERATE id,login,full_name;
users_no_tweets = FOREACH (FILTER joined BY id IS NULL) GENERATE full_name;
DUMP users_no_tweets;
