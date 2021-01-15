/* Loading flat file with pig to HBASE database using phoenix client
 * this file is avaiable at: http://media.sundog-soft.com/hadoop/phoenix.pig
 */
REGISTER /usr/hdp/current/phoenix-client/phoenix-client.jar

users = LOAD '/user/maria_dev/ml-100/u.user'
USING PigStorage('|')
AS (USERID:int, AGE:int, GENDER:chararray, OCCUPATION:chararray, ZIP:chararray);

STORE users into 'hbase://users' using
    org.apache.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 5000');

occupations = load 'hbase://table/users/USERID,OCCUPATION' using org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');

grpd = GROUP occupations BY OCCUPATION;
cnt = FOREACH grpd GENERATE group AS OCCUPATION,COUNT(occupations);
DUMP cnt;
