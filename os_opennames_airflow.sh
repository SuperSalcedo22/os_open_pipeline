#!/bin/bash

# example input
# bash ./os_openname_airflow.sh <pg_password> <filename>

# Functions
function exitmesg { mesg=$1; echo "${mesg}" >&2; exit 1; }
function warningmesg { mesg=$1; echo "${mesg}" >&2; }
function pgconnect { PGPASSWORD=${pgpassword} psql -U ${pguser} -h ${pghost} -t -d ${pgdb}; }
function pgquery { query=$1; pgconnect <<< "${query}"; }
function get_variables {
    # Specify the path to the .env file
    envpath=".env"
    
    # Check if the .env file exists and assign variables to it
    if [ -f "$envpath" ]; then
        source "$envpath"
        pguser="$PG_USER"
        pghost="$PG_HOST"
        pgdb="$PG_DB"
        folder_path="$FOLDER_PATH"
    else
        exitmsg "Error: .env file not found."
    fi
    pgpassword=$1
    pgschema=os_opennames
    filename=$2
    path=$folder_path/OpenNames
}
function check_counts {
    # first input is the current table, 2nd is the load 
    output=$(pgquery "SET ROLE $pguser; SELECT $pgschema.check_counts_within_10_percent_csv('$pgschema','$1','$2',$rowcount);")
    # Remove all non-numerical characters
    int_var="${output//[^0-9]/}"
    if [[ $int_var -ne 1 ]]
    then
        # exit the script as there is something wrong with the loaded data
        exitmesg "$2 failed checks, exiting script please double check"
    fi
}
function append_log {
    completion_time=$(date "+%Y-%m-%d %H:%M:%S")
    text="$completion_time - Tables in postgres under $pgschema have been updated"
    file_path="$folder_path/logs/database_log.txt"
    echo "$text" >> "$file_path"
}

function check_data {
    # the csv doesn't come with the headers, so can't compare them

    # check the number of columns match by comparing against the database
    output=$(pgquery "SELECT count(column_name) FROM information_schema.columns WHERE table_schema = '$pgschema' AND table_name = 'os_opennames';" ) 
    # Remove all non-numerical characters
    db_col_count="${output//[^0-9]/}"
    # minus 1 as the csv doesn't have a geom column
    db_col_count=$(($db_col_count - 1))

    # check the number of columns on the csv (assuming no commas in the column names)
    data_col_count=$(head -n 1 $path/opennames_ALL_$filedate.csv | tr ',' '\n' | wc -l)

    if [ $data_col_count -eq $db_col_count ]; then
        echo "Column counts match, continuing"
    else
        exitmesg "Mismatching number of columns, Postgres: $db_col_count, File: $data_col_count - please check"
    fi
}

function copy_data {
    echo "Concatenating files"
    # First, find the date of the file to be used
    filedate=$(echo "$filename" | awk -F'_' '{print $NF}')
    cat $path/Updates/$filename/Data/*.csv > $path/opennames_ALL_$filedate.csv
    declare -g row_count # make it a global variable
    row_count=$(wc -l < $path/opennames_ALL_$filedate.csv)
    echo "Files concatenated and copied"
}

function load_tables {
    echo "Generating load table"
    # creating and loading the table
    pgquery "DROP TABLE IF EXISTS $pgschema.os_opennames_load;
        CREATE TABLE $pgschema.os_opennames_load (LIKE $pgschema.os_opennames);ALTER TABLE $pgschema.os_opennames_load DROP COLUMN geom;
        \COPY $pgschema.os_opennames_load FROM '$path/opennames_ALL_$filedate.csv' csv;"
    echo "Tables created within postgres"
}
function commit2database {
    echo "Adding geometry column"
    pgquery "ALTER TABLE $pgschema.os_opennames_load ADD COLUMN geom geometry (Point, 27700); UPDATE $pgschema.os_opennames_load SET geom = ST_SetSRID(ST_MakePoint(geometry_x, geometry_y), 27700);"
    # changing the roles
    echo "Altering roles for the table"
    pgquery "SET ROLE $pguser; ALTER TABLE $pgschema.os_opennames_load ADD PRIMARY KEY (id);"
    echo "Roles granted, creating indexes"

    #indexes
    pgquery "CREATE INDEX ON $pgschema.os_opennames_load USING btree (lower(name1::text) COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST) TABLESPACE pg_default;
        CREATE INDEX ON $pgschema.os_opennames_load USING gist (geom) TABLESPACE pg_default;
        CREATE INDEX ON $pgschema.os_opennames_load USING btree (name1 COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;
        CREATE INDEX ON $pgschema.os_opennames_load USING btree (name2 COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;
        CREATE INDEX ON $pgschema.os_opennames_load USING btree (type COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;"

    # finalising the update
    pgquery "DROP TABLE $pgschema.os_opennames_old; ALTER TABLE $pgschema.os_opennames RENAME TO os_opennames_old; ALTER TABLE $pgschema.os_opennames_load RENAME TO os_opennames;"
}

function main {
    if [ "$#" -ne 3 ]; then
        echo "Error: Insufficient number of arguments. Expected 2 arguments."
        echo "Usage: $0 pg_password filename"
        exit 1
    fi
    get_variables
    copy_data
    check_data
    load_tables
    check_counts "os_opennames" "os_opennames_load"
    commit2database
    echo 'Update Complete'
    append_log
    exit 0
}

main "$@"
$SHELL
