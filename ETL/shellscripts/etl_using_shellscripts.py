# # code for the postgreSQL CLI #postgres
# \l #to check databses in PostgreSQL
# #to connect to a database
# \c database-name
# create table users(username varchar(10), userid int, homedirectory varchar(100));
# \dt # tolist the tables present in a database
# touch csv2db.sh

# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.

# Extract phase

echo "Extracting data"

# Extract the columns 1 (user name), 2 (user id) and 
# 6 (home directory path) from /etc/passwd

cut -d":" -f1,3,6 /etc/passwd

cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt

# Transform phase
echo "Transforming data"
# read the extracted data and replace the colons with commas.

tr ":" "," < extracted-data.txt  > transformed-data.csv

# Load phase
echo "Loading data"
# Set the PostgreSQL password environment variable.
# Replace <yourpassword> with your actual PostgreSQL password.
export PGPASSWORD=<yourpassword>;
# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.
echo "\c template1;\COPY users  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=postgres

echo "SELECT * FROM users;" | psql --username=postgres --host=postgres template1
