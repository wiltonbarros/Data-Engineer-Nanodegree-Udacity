<h2><b>Introduction</b></h2>
<p>
    The objective of this project is to ETL a dataset in JSON format stored in an AWS S3 bucket for the Sparkify startup music project, the steps consist of extracting the JSON files to two stage tables on a base redshift, from this structure build fact and dimension tables for correct data loading in each table.
    </p>
<hr></hr>

<h2><b>Structure</b></h2>

The structure of this project consists of a json file containing information regarding the songs and another json file with user information.
From these files we create the star schema tables in redshift being:

<b>Fact:</b> Songplays

<b>Dimensions:</b> users, songs, artists and time.

<hr></hr>


<h2><b>Steps</b></h2>

For the execution of this project, the content of the json files was extracted into two staging tables, being staging_songs and staging_events.
From these tables, we transform the data for each dimension and fact of the data model and then load it into each created table.
