<h2><b>Introduction</b></h2>

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


<h2><b>Database description</b></h2>

The structure chosen for this project was the star schema, where we have a dimension table and several fact tables.
Below the definition of each table within the schema.

<h2><b>Tables</b></h2>
    
<h3><b>Fact Tables</b></h3>

<ul><li><b>Songplays - </b> Save all records with users, songs, artists informations.</li></ul>

<h3><b> Dimension Tables </b></h3></li></ul>

<ul><li><b> Users - </b> Users registered in app</li></ul>

<ul><li><b> Songs - </b> Songs in use in app</li></ul>

<ul><li><b> Artists - </b> Artists registered in app</li></ul>

<ul><li><b> Time - </b> Timestamp registered in app by any songs.</li></ul>


<h3><b>ETL Process</b></h3>

For this project, we must ETL json files separated into 2 directories and transform them into a star schema structure in a postgres database, defining the fields and data types of each table.
To create tables, deletions and insertions, we use the command "create_tables.py" and "python etl.py" to assemble the database structure and perform the ETL/insertions.


<h3><b> Relevant files in project </b></h3>

<b> create_table.py -</b> Commands to delete and recreate tables and database.
<b> test.ipnb -</b> File to run the tests in the project
<b> sql_queries -</b> File with commands (create table, insert and select in database).
<b> etl.ipynb -</b> File to perform the initial ETL tests and data entry.
<b> etl.py -</b> File that runs the entire process of creating ETL and inserting data into the database.


