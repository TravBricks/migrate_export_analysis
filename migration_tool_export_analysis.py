import glob
import re
import argparse
import os

def prog_arg_parser():
    # export workspace items
    parser = argparse.ArgumentParser(
        description='Analyze migration tool exported metastore table/view DDL statements and in support of table data migration tasks.')

    parser.add_argument('--migrate_metastore_export', action='store', default='check_string_for_empty',
                    help='Path of migration tool export (note:will recurse)')

    parser.add_argument('-p','--sessionidpath', action='store', default='check_string_for_empty',
                    help='SESSION_ID File Path, interchangeable with --migrate_metastore_export')

    parser.add_argument('--hive_path', action='store', default="/user/hive/warehouse/",
                        help='The dbfs path where default managed databases and tables are written (should end with a \'/\')  (default: %(default)s)')

    parser.add_argument('--staging_path', action='store', default="/mnt/lnd/devmetastore_migration/",
                        help='The absolute mount path of the staging bucket (default: %(default)s)')

    parser.add_argument('--output_path', action='store', default='./output/',
                            help='Path to outputfile location')

    parser.add_argument('--details_format', action='store', choices=['csv','json'], default='json',
                            help='Define detail output format (default: %(default)s)')

    parser.add_argument('--show_all', action='store_true',
                            help='Include all show commands: summary, details, issues, deep, ctas and cleanup)')

    parser.add_argument('--show_summary', action='store_true',
                        help='Provides a summary of our analysis')

    parser.add_argument('--show_details', action='store_true',
                            help='Outputs a full list of tables analyzed')

    parser.add_argument('--show_issues', action='store_true',
                            help='Outputs a list of tables with non-standard using/format and other checks')

    parser.add_argument('--show_deep', action='store_true',
                            help='Outputs DDL DEEP CLONE statements')

    parser.add_argument('--show_ctas', action='store_true',
                            help='Outputs CTAS Table Copy statements')
    
    parser.add_argument('--show_cleanup', action='store_true',
                            help='Outputs environment cleanup commands')

    parser.add_argument('--show_config', action='store_true',
                            help='Show configuation details')

    
    return parser

class prog_config:
    outputfiles = ["config.txt","summary.txt","details.txt","legacyissues.txt","source_env_deep.txt","source_env_ctas.txt","target_env_deep.txt","target_env_ctas.txt","source_env_cleanup.txt","target_env_cleanup.txt"]

    def __init__(self):
        self.export_path = ""
        self.hive_path = ""
        self.staging_path = ""
        self.show_summary = False
        self.show_details = False
        self.show_issues = False
        self.show_deep = False
        self.show_ctas = False
        self.show_config = False
        self.show_cleanup = False
        self.details_format = None
        self.logging = {}
        self.output_path = ""
        
    def loadArgs(self):
        parser=prog_arg_parser()
        args=vars(parser.parse_args())

        # We need either a sessionID or a export_path
        if args['sessionidpath'] != 'check_string_for_empty':
            #What path are we analyzing
            self.export_path=args['sessionidpath'] + "/metastore/*/*"
        
        if args['migrate_metastore_export'] != 'check_string_for_empty':
            self.export_path=args['migrate_metastore_export']

        if self.export_path.find("*") == -1:
            if self.export_path[-1] != "/": self.export_path+="/"
            self.export_path+="*/*"

        if self.export_path == "":
            print ("Either --migrate_metastore_export or --sessionidpath must be defined!")
            exit(1)

        self.hive_path=args['hive_path']
        if self.hive_path[-1] != "/": self.hive_path+="/"        
        self.staging_path=args['staging_path']
        self.details_format=args['details_format']
        self.output_path=args['output_path']

        if args['show_all']:
            self.show_summary=True
            self.show_details=True
            self.show_issues=True
            self.show_deep=True
            self.show_ctas=True
            self.show_cleanup=True
            self.show_config=True
        else:
            self.show_summary=args['show_summary']
            self.show_details=args['show_details']
            self.show_issues=args['show_issues']
            self.show_deep=args['show_deep']
            self.show_ctas=args['show_ctas']
            self.show_cleanup=args['show_cleanup']
            self.show_config=args['show_config']

        self.logging = dict.fromkeys(self.outputfiles, False)
    
    def logger(self,filename,content):
        
        #check for path
        if os.path.exists(self.output_path) == False:
            os.mkdir(self.output_path)

        #check if file has been intialized
        if not self.logging[filename]:
            opentype = "w"
            self.logging[filename]=True
        else:
            opentype = "a"

        try:
            f = open(f"{self.output_path}/{filename}", opentype)
            f.writelines(content)
            f.close()
        except:
            print(f"Unable to open/write to log file '{self.output_path}/{filename}'")


def ddl_files(path="."):
    extracted=[]
    
    # Returning an iterator that will print simultaneously.
    for fp in glob.iglob(path, recursive = True):
        #print(fp)
        f = open(fp,'r')
        extracted.append(ddl_extract(f.read()))

    if len(extracted) == 0: print(f"No DDL statements were located in path '{path}'")

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
            filename = "source_env_deep.txt"
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
            filename = "target_env_deep.txt"
            ddlstatement = f"""
    ----------------------------------------------------------------------
    --Deep: For table {deltadb}.{tablename}: Load staging delta location as a table
        
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
        myconfig.logger(filename,ddlstatement)
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

            filename = "source_env_ctas.txt"
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
            filename = "target_env_ctas.txt"
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
        SELECT * FROM {using}.`{ctassqltablepath}`;
        """
        
        myconfig.logger(filename,ddlstatement)
        print(ddlstatement)

    return None

def cleanup_build(cleanupobjs,migrate_prefix="staging_", stagingpath=""):
    
    dblist = []
    for obj in allobjs:
        if obj['database'] in dblist: continue
        dblist.append(obj['database'])

    print(dblist)

    for db in dblist:
        stagingdb = f"{migrate_prefix}{db}"
        ddlstatement = f"""
    ----------------------------------------------------------------------
    --Clean-up: Remove staging {stagingdb} database and all objects 
       DROP DATABASE IF EXISTS {stagingdb} CASCADE;
        """
        myconfig.logger("source_env_cleanup.txt",ddlstatement)
        myconfig.logger("target_env_cleanup.txt",ddlstatement)
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
                obj['reason'] = f"Invalid table format is '{obj['using']}' needs to be {valid_using}"
                obj['result'] = "Table will not be copied"
                
            if obj['using'].lower() == "text":
                obj['reason'] = f"Warning table format TEXT only supports string field types"
                obj['result'] = "Table copy may fail"

            if 'reason' in obj:
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

    summarytext = f"""
    === Analysis Summary ===
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
    """

    myconfig.logger("summary.txt",summarytext)
    print(summarytext)

def show_config(config):
    configtext = f"""
    === Configuration Summary ===
    Paths:
        Export: {config.export_path}
        Hive: {config.hive_path}
        Staging: {config.staging_path}
        Output: {config.output_path}

    Display:
        Show Summary: {config.show_summary}
        Show Details: {config.show_details}
            Format {config.details_format}
        Show DEEP: {config.show_deep}
        Show CTAS: {config.show_ctas}
    """
    myconfig.logger("config.txt",configtext)
    print(configtext)


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

    if myconfig.show_issues:
        legacyissues = f"=== Legacy or Issue Tables ===\n"
        legacyissues+='\n'.join(f"\t{line['database']}.{line['table']}: {line['result']} - {line['reason']}" for line in problemobjs)
        print(legacyissues)
        myconfig.logger("legacyissues.txt",legacyissues)

    # display details as stdout
    if myconfig.show_details:
        details = '\n'.join(str(line) for line in allobjs)
        print(f"\n\n=== Table Details ===")
        print(details)
        myconfig.logger("details.txt",details)
    
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

    if myconfig.show_cleanup:
        print(f"\n\n=== Cleanup: Drop all staging databases and tables ===")

        print(f"\n-- Cleanup: Source environment --")
        cleanup_build(allobjs,migrate_prefix="staging_", stagingpath=myconfig.staging_path)

        print(f"\n-- Cleanup: Target environment --")
        cleanup_build(allobjs,migrate_prefix="staging_", stagingpath=myconfig.staging_path)

    # fin