/*https://www.vistrails.org/index.php/Assignment_4_-_Querying_with_Pig_and_Mapreduce*/
/*1a*/
users = LOAD 'pig_examples/users.csv' USING PigStorage(',') AS (login:chararray, full_name:chararray, state:chararray);
ny_users = FOREACH (FILTER users BY (LOWER(state) == 'ny')) GENERATE login;
DUMP ny_users;
