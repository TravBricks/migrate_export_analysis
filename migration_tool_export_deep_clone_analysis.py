import glob
import re
import argparse
from attr import has
import pandas

def prog_arg_parser():
    # export workspace items
    parser = argparse.ArgumentParser(
        description='Analyze migration tool exported metastore table/view DDL statements')

    parser.add_argument('--migrate_metastore_export', action='store', default='check_string_for_empty',
                    help='Location of migration tool export (note:will recurse')

    parser.add_argument('-p','--sessionidpath', action='store', default='check_string_for_empty',
                    help='SESSION_ID File Path')

    parser.add_argument('--hive_path', action='store', default="/user/hive/warehouse/",
                        help='The dbfs path where default managed databases and tables are written (should end with a \'/\'')

    parser.add_argument('--staging_path', action='store', default="/mnt/lnd/devmetastore_migration/",
                        help='The absolute mount path of the staging bucket')

    parser.add_argument('--show_summary', action='store_true',
                        help='Provides a summary of our analysis')

    parser.add_argument('--show_details', action='store_true',
                            help='Outputs a full list of tables analyzed')

    parser.add_argument('--details_format', action='store', choices=['csv','json'], default='json',
                            help='Define detail output format')

    parser.add_argument('--details_output', action='store',
                            help='Writes details to an output formatted as defined by --details_format')

    parser.add_argument('--show_deep', action='store_true',
                            help='Outputs DDL DEEP CLONE statements')

    parser.add_argument('--show_ctas', action='store_true',
                            help='Outputs CTAS Table Copy statements')
    
    parser.add_argument('--show_config', action='store_true',
                            help='Show configuation details')


    return parser


class prog_config:
    def __init__(self):
        self.export_path = ""
        self.hive_path = ""
        self.staging_path = ""
        self.show_summary = False
        self.show_details = False
        self.show_deep = False
        self.show_ctas = False
        self.show_config = False
        self.details_format = None
        self.details_output = None
        
    def loadArgs(self):
        parser=prog_arg_parser()
        args=vars(parser.parse_args())

        # We need either a sessionID or a export_path
        if args['sessionidpath'] != 'check_string_for_empty':
            #What path are we analyzing
            self.export_path=args['sessionidpath'] + "/metastore/*/*"
        
        if args['migrate_metastore_export'] != 'check_string_for_empty':
            self.export_path=args['migrate_metastore_export']

        if self.export_path == "":
            print ("Either --migrate_metastore_export or --sessionidpath must be defined!")
            exit(1)

        self.hive_path=args['hive_path']
        self.staging_path=args['staging_path']

        self.show_summary=args['show_summary']

        self.show_details=args['show_details']
        self.details_format=args['details_format']
        self.details_output=args['details_output']

        self.show_deep=args['show_deep']
        self.show_ctas=args['show_ctas']

        self.show_config=args['show_config']

def ddl_files(path="."):
    extracted=[]
    
    # Returning an iterator that will print simultaneously.
    for fp in glob.iglob(path, recursive = True):
        #print(fp)
        f = open(fp,'r')
        extracted.append(ddl_extract(f.read()))

    return extracted

def ddl_extract(ddlcmd):
    ptypedbtble = re.compile(r"CREATE (?P<type>TABLE|VIEW) (spark_catalog\.)?(?P<database>[\w\-\_]+)\.(?P<table>[\w\-\_]+)(.+)")
    pusing = re.compile(r"USING (?P<using>[\w\.]+)")
    ploc = re.compile(r"(LOCATION \'(?P<location>(.*?))\')")

    objdetails = {}
    m = ptypedbtble.search(ddlcmd)
    if m != None:
        objdetails=m.groupdict()

    m = pusing.search(ddlcmd)
    if m != None:
        objdetails.update(m.groupdict())

    m = ploc.search(ddlcmd)
    if m != None:
        objdetails.update(m.groupdict())

    #retain DDL for CTAS
    objdetails.update({'ddlcmd':ddlcmd})

    return objdetails

def deepclone_build(buildobjs,migrate_prefix="staging_",direction="staging", stagingpath=""):

    for obj in buildobjs:
        deltadb = obj['database']
        deepdb = f"{migrate_prefix}{obj['database']}"
        tablename = obj['table']
        
        sourcelocation = obj['location']

        deepsqltablepath = f"{stagingpath}{deepdb}.db/{tablename}"

        if direction == "staging":
            ddlstatement = f"""
    ----------------------------------------------------------------------
    --Deep: For table {deltadb}.{tablename}: Deep clone delta table to staging
       
       -- [1/2] Create staging database
       CREATE DATABASE IF NOT EXISTS {deepdb};
       
       -- [2/2] Deep Clone data to staging
       CREATE OR REPLACE TABLE {deepdb}.{tablename}
       DEEP CLONE {deltadb}.{tablename}
       LOCATION '{deepsqltablepath}';
            """
        else:
            ddlstatement = f"""
    ----------------------------------------------------------------------
    --Deep: For table {deltadb}.{tablename}: Load delta location as a table
        
        -- [1/4] Create staging database
        CREATE DATABASE IF NOT EXISTS {deepdb};

        -- [2/4] Register staging table
        CREATE TABLE IF NOT EXISTS {deepdb}.{tablename} AS
        SELECT * FROM delta.`{deepsqltablepath}`;

        -- [3/4] Create target database
        CREATE DATABASE IF NOT EXISTS {deltadb};
        
        -- [4/4] Deep clone staging to target table
        CREATE OR REPLACE TABLE {deltadb}.{tablename}
        DEEP CLONE {deepdb}.{tablename}
        LOCATION '{sourcelocation}';
            """

        print(ddlstatement)

    return None

def ctascopy_build(buildobjs,migrate_prefix="staging_",direction="staging", stagingpath=""):

    for obj in buildobjs:
        database = obj['database']
        ctasdb = f"{migrate_prefix}{obj['database']}"
        tablename = obj['table']
        using = obj['using']
        
        sourcelocation = obj['location']

        ctassqltablepath = f"{stagingpath}{ctasdb}.db/{tablename}"

        ddlcmd = obj['ddlcmd']
        
        ddllines = ddlcmd.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS").split("\n")
        hasLocation = ddlcmd.find('LOCATION')>0
        ddlindent = "\t\t\t\t"

        if direction == "staging":
            sourceDDL = ""
            for l, line in enumerate(ddllines):
                if len(line) == 0: break
                newline = line
                newline = newline.replace(database,ctasdb)
                if hasLocation == False and newline[0:5] == "USING":
                    newline+=f"\n{ddlindent}LOCATION '{ctassqltablepath}'"
                sourceDDL+=f"\n{ddlindent}{newline}"

            ddlstatement = f"""
    ----------------------------------------------------------------------
    -- CTAS: For table {database}.{tablename}: Source to staging location
        
        -- [1/3] Create staging database
        CREATE DATABASE IF NOT EXISTS {ctasdb};
        
        -- [2/3] Define staging table schema/location
        {sourceDDL};

        -- [3/3] Insert into staging table from source table
        INSERT INTO {ctasdb}.{tablename}
        SELECT * FROM {database}.{tablename};
            """
        else:
            targetDDL = ""
            for l, line in enumerate(ddllines):
                if len(line) == 0: break
                newline = line
                targetDDL+=f"\n{ddlindent}{newline}"

            ddlstatement = f"""
    ----------------------------------------------------------------------
    -- CTAS: For table {ctasdb}.{tablename}: Copy data from staging to target
        
        -- [1/3] Create target database
        CREATE DATABASE IF NOT EXISTS {database};
        
        -- [2/3] Define target table
        {targetDDL};

        -- [3/3] Insert into target table with select from staging
        INSERT INTO {database}.{tablename}
        SELECT * FROM {using}.'{ctassqltablepath}';
        """

        print(ddlstatement)

    return None


def build_location(hivepath,database,table):
    location = ""
    if database.lower() == "default":
        # default tables are written to metastore root
        location = f"{hivepath}{table}"
    else:
        location = f"{hivepath}{database}.db/{table}"

    return location


def deepclone_candidates(objdict,hivepath):
    candidates = []

    for obj in objdict:
        try:
            if obj['type'] == "TABLE" and obj['using'].lower() == "delta":
                if "location" not in obj: 
                    obj['location'] = build_location(hivepath,obj['database'],obj['table'])
                
                if obj['location'][0:len(hivepath)] == hivepath:
                    candidates.append(obj)

        except Exception as e:
            print(f"[ERROR] Probably not a delta table! {obj}")
            pass

    return candidates


def ctascopy_candidates(objdict,hivepath):
    candidates = []
    ctas_using = ["TEXT","AVRO","CSV","JSON","PARQUET","ORC"]
    for obj in objdict:
        try:
            if obj['type'] == "TABLE":

                if "location" not in obj:
                    obj['location'] = build_location(hivepath,obj['database'],obj['table'])
            
                if obj['using'].lower() in map(str.lower, ctas_using):
                    if obj['location'][0:len(hivepath)] == hivepath:
                        candidates.append(obj)
                        
        except Exception as e:
            print(f"[ERROR] Probably not a CTAS candidate! {obj}")
            pass

    return candidates

def problem_tables(objs,hivepath):
    problems = []
    valid_using = ["TEXT","AVRO","CSV","JSON","PARQUET","ORC","DELTA"]
    for obj in objs:
        if obj["type"] == "TABLE":
            if obj['using'].lower() not in map(str.lower, valid_using):
                # print(f"[WARNING] {obj['database']}.{obj['table']} has problematic 'USING' {obj['using']}")
                problems.append(obj)

    return problems


def show_summary(objs,hivepath):
    total = len(objs)
    views = 0
    tables = 0
    table_nondelta_hive = 0
    table_nondelta_ext = 0
    table_delta_hive = 0
    table_delta_ext = 0

    for obj in objs:
        if obj["type"] == "VIEW": 
            views+=1
        else:
            tables+=1
            if obj['using'].lower() == "delta":
                if obj['location'][0:len(hivepath)] == hivepath:
                    table_delta_hive+=1
                else:
                    table_delta_ext+=1
            else:
                if obj['location'][0:len(hivepath)] == hivepath:
                    table_nondelta_hive+=1
                else:
                    table_nondelta_ext+=1

    print("\n\n=== Analysis Summary ===")
    print(f"""
    Total DDL statements: {total}
    
    Types:
        Tables: {tables}
        Views: {views}
    
    Managed Tables ({hivepath}):
        Delta: {table_delta_hive} Deep Clone candidates
        Other: {table_nondelta_hive} CTAS Copy candidates
    
    External Tables:
        Delta: {table_delta_ext}
        Other: {table_nondelta_ext}
    """)

def show_config(config):
        print(f"\n=== Configuration Summary ===")
        print(f"""
        Paths:
            Export: {config.export_path}
            Hive: {config.hive_path}
            Staging: {config.staging_path}
        
        Display:
            Show Summary: {config.show_summary}
            Show Details: {config.show_details}
                Format {config.details_format}
                Output {config.details_output}
            Show DEEP: {config.show_deep}
            Show CTAS: {config.show_ctas}
        """)


if __name__ == "__main__":
    myconfig = prog_config()
    myconfig.loadArgs()

    # show config settings
    if myconfig.show_config: show_config(myconfig)

    # extract details from ddl files
    allobjs = ddl_files(myconfig.export_path)

    # process for deepclone and ctas candidates
    deepobjs = deepclone_candidates(allobjs, myconfig.hive_path)
    ctasobjs = ctascopy_candidates(allobjs, myconfig.hive_path)
    problemobjs = problem_tables(allobjs, myconfig.hive_path)

    # display summary of the analysis
    if myconfig.show_summary: show_summary(allobjs, myconfig.hive_path)

    print("\n=== Legacy or Issue Tables ===")
    print('\n'.join(f"\t{line['database']}.{line['table']} using is {line['using']}" for line in problemobjs))

    # generate details in defined format (defaults to json)
    if myconfig.details_format == "csv":
        df = pandas.DataFrame(allobjs)
        details = df.to_csv(index=False)
    else:
        details = '\n'.join(str(line) for line in allobjs)

    # display details as stdout
    if myconfig.show_details:
        print(f"\n\n=== Table Details ===")
        print(details)

    # write details to defined file
    if myconfig.details_output is not None:
        try:
            f = open(myconfig.details_output, "w")
            f.writelines(details)
            f.close
        except:
            print(f"Could not create or write to {myconfig.details_output}, exiting application.")
            exit(1)
    
    # show deep clone candidates with DDL
    if myconfig.show_deep:
        print(f"\n\n=== DEEP CLONE DDL Statements ===")

        print(f"\n-- DEEP: Source -> Staging [Run in Source Environment] --")
        deepclone_build(deepobjs,migrate_prefix="staging_",direction="staging", stagingpath=myconfig.staging_path)

        print(f"\n-- DEEP: Staging -> Target [Run in Target Environment] --")
        deepclone_build(deepobjs,migrate_prefix="staging_",direction="target", stagingpath=myconfig.staging_path)

    # show CTAS candidates with DDL
    if myconfig.show_ctas:
        print(f"\n\n=== CREATE TABLE AS (CTAS) DDL Statements ===")

        print(f"\n-- CTAS: Source -> Staging --")
        ctascopy_build(ctasobjs,migrate_prefix="staging_",direction="staging", stagingpath=myconfig.staging_path)

        print(f"\n-- CTAS: Staging -> Target --")
        ctascopy_build(ctasobjs,migrate_prefix="staging_",direction="target", stagingpath=myconfig.staging_path)

    # fin