# Importing from MySQL to Hive 
sqoop import --connect jdbc:mysql://localhost/movielens --username root -P --driver com.mysql.jdbc.Driver --table movies -m 1 --hive-import

# Exporting from Hive to MySQL 
# Before run this we need to create that "exported_movies" table in MySQL 
sqoop export --connect jdbc:mysql://localhost/movielens --username root -P -m 1 --driver com.mysql.jdbc.Driver --table exported_movies --export-dir /apps/hive/warehouse/movies --input-fields-terminated-by '\0001'

