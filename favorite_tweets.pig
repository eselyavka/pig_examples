/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*1b*/
tweets = LOAD 'pig_examples/tweets.csv' USING PigStorage(',') AS (id:long, payload:chararray, login:chararray);
favorite_tweets = FOREACH (FILTER tweets BY (LOWER(payload) MATCHES '.*favorite.*')) GENERATE id,payload;
ordered_ft = FOREACH (ORDER favorite_tweets BY id) GENERATE payload;
DUMP ordered_ft;
