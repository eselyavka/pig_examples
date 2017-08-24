/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*3a*/
tweets = LOAD 'pig_examples/tweets.csv' USING PigStorage(',') AS (id:int, payload:chararray, login:chararray);
users = LOAD 'pig_examples/users.csv' USING PigStorage(',') AS (ulogin:chararray, full_name:chararray, state:chararray);
joined = FOREACH (JOIN tweets BY login, users BY ulogin) GENERATE id,login,full_name;
number_of_tweets_per_user = FOREACH (GROUP joined BY login) {
    unique_full_name = DISTINCT joined.full_name;
    GENERATE
        FLATTEN(unique_full_name) AS ufn,
        COUNT(joined.full_name) AS cnt;
};
DUMP number_of_tweets_per_user;

/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*3b*/
number_of_tweets_per_user_ordered = ORDER number_of_tweets_per_user BY cnt DESC;

DUMP number_of_tweets_per_user_ordered;
