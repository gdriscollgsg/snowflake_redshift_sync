#import argparse
import datetime
import json
import sys

import snowflake.connector

# Constants
# Snowflake constants
SNOWFLAKE_ROLE_NAME = None
SNOWFLAKE_WAREHOUSE_NAME = None
SNOWFLAKE_DATABASE_NAME = None
SNOWFLAKE_SCHEMA_NAME = None
SNOWFLAKE_FILE_FORMAT = None

def main():
    startDateTime = datetime.datetime.now()
    print("Started: ", startDateTime)

#    parser = argparse.ArgumentParser()
#    parser.add_argument("--sql", help="The SQL query to execute, enclose in double quotes")
#    parser.add_argument("--sql_params", help="The SQL parameters necessary for the SQL query to execute. Use key=value syntax and separate each with a semi-colon, i.e. myparam=xyz;myparam2=abc")
#    args = parser.parse_args()

    with open('tosf.cfg') as config_data_file:
        config_data = json.load(config_data_file)

    SNOWFLAKE_ROLE_NAME = config_data["snowflake"]["role"]
    SNOWFLAKE_WAREHOUSE_NAME = config_data["snowflake"]["warehouse"]
    SNOWFLAKE_DATABASE_NAME = config_data["snowflake"]["database"]
    SNOWFLAKE_SCHEMA_NAME = config_data["snowflake"]["schema"]
    SNOWFLAKE_ACCOUNT = config_data["snowflake"]["account"]
    SNOWFLAKE_REGION = config_data["snowflake"]["region"]
    SNOWFLAKE_USERNAME = config_data["snowflake"]["username"]
    SNOWFLAKE_PASSWORD = config_data["snowflake"]["password"]
#    SQL = args.sql
#    SQL_PARAMS = args.sql_params.split(';')

    # Gets the version, a simple connectivity test, if we get the version then we're good to proceed
    try:
        ctx = snowflake.connector.connect(
            user = SNOWFLAKE_USERNAME,
            password = SNOWFLAKE_PASSWORD,
            account = SNOWFLAKE_ACCOUNT,
            region = SNOWFLAKE_REGION)
        cs = ctx.cursor()
        print("connected")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        cs.close()
        sys.exit(1)

    try:
        cs.execute("SELECT current_version()")
        one = cs.fetchone()
        print(one[0])
    except:
        print("Error retrieving current version!")
        cs.close()
        sys.exit(1)


if __name__ == "__main__":
    main()