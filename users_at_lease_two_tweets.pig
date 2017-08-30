/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*4a*/
tweets = LOAD 'pig_examples/tweets.csv' USING PigStorage(',') AS (id:long, payload:chararray, login:chararray);
users = LOAD 'pig_examples/users.csv' USING PigStorage(',') AS (ulogin:chararray, full_name:chararray, state:chararray);
joined = FOREACH (JOIN tweets BY login, users BY ulogin) GENERATE id,login,full_name;
number_of_tweets_per_user = FOREACH (GROUP joined BY login) {
    unique_full_name = DISTINCT joined.full_name;
    GENERATE
        FLATTEN(unique_full_name) AS ufn,
        COUNT(joined.full_name) AS cnt;
};
user_with_at_least_two_tweets = FOREACH (FILTER number_of_tweets_per_user BY cnt >= 2) GENERATE ufn;
DUMP user_with_at_least_two_tweets;
