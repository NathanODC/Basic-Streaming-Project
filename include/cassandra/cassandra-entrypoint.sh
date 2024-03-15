#!/bin/bash
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_streams WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
cqlsh -e "CREATE TABLE IF NOT EXISTS spark_streams.created_users (id UUID PRIMARY KEY, first_name TEXT, last_name TEXT, gender TEXT, address TEXT, post_code TEXT, email TEXT, username TEXT, registered_date TEXT, phone TEXT, picture TEXT);"