from ast import Constant
import glob
import re
import argparse

def prog_arg_parser():
    # export workspace items
    parser = argparse.ArgumentParser(
        description='Analyze migration tool exported metastore table/view DDL statements')

    parser.add_argument('--migrate_metastore_export', action='store', default='check_string_for_empty',
                    help='Location of migration tool export (note:will recurse')

    parser.add_argument('-p','--sessionidpath', action='store', default='check_string_for_empty',
                    help='SESSION_ID File Path')

    parser.add_argument('--hive_path', action='store', default="/user/hive/warehouse/",
                        help='The dbfs path where default managed databases and tables are written')

    parser.add_argument('--staging_path', action='store', default="/mnt/lnd/devmetastore_migration/",
                        help='The absolute mount path of the staging bucket.')

    parser.add_argument('--show_summary', action='store_true',
                        help='Provides a summary of our analysis')

    parser.add_argument('--show_details', action='store_true',
                            help='Outputs a full list of tables analyzed')

    parser.add_argument('--show_deep', action='store_true',
                            help='Outputs DDL DEEP CLONE statements')

    parser.add_argument('--show_ctas', action='store_true',
                            help='Outputs CTAS Table Copy statements')

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
            print ("SESSION_ID File Path is not valid!")
            exit(1)


        self.hive_path=args['hive_path']
        self.staging_path=args['staging_path']

        self.show_summary=args['show_summary']
        self.show_details=args['show_details']
        self.show_deep=args['show_deep']
        self.show_ctas=args['show_ctas']


def ddl_extract(path="."):
    extracted=[]

    ptypedbtble = re.compile(r"CREATE (?P<type>TABLE|VIEW) (spark_catalog\.)?(?P<database>[\w\-\_]+)\.(?P<table>[\w\-\_]+)(.+)")
    pusing = re.compile(r"USING (?P<using>\w+)")
    ploc = re.compile(r"(LOCATION \'(?P<location>(.*?))\')")


    # Returning an iterator that will print simultaneously.
    for fp in glob.iglob(path, recursive = True):
        #print(fp)
        f = open(fp,'r')
        testsql = f.read()

        objdetails = {}
        m = ptypedbtble.search(testsql)
        if m != None:
            objdetails=m.groupdict()

        m = pusing.search(testsql)
        if m != None:
            objdetails.update(m.groupdict())

        m = ploc.search(testsql)
        if m != None:
            objdetails.update(m.groupdict())
        
        # print(f"\nLength:{len(objdetails)} {objdetails}")
        extracted.append(objdetails)
        
    return extracted

def deepclone_build(buildobjs,migrate_prefix="staging_",direction="staging", stagingpath=""):
    

    for obj in buildobjs:
        deltadb = obj['database']
        deepdb = f"{migrate_prefix}{obj['database']}"
        tablename = obj['table']
        
        sourcelocation = obj['location']

        deepsqltablepath = f"{stagingpath}{deepdb}.db/{tablename}"

        if direction == "staging":
            ddlstatement = f"""
            CREATE DATABASE IF NOT EXISTS {deepdb};
            CREATE OR REPLACE TABLE {deepdb}.{tablename}
            DEEP CLONE {deltadb}.{tablename}
            LOCATION '{deepsqltablepath}';
            """
        else:
            ddlstatement = f"""
            CREATE DATABASE IF NOT EXISTS {deltadb};
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

        deepsqltablepath = f"{stagingpath}{ctasdb}.db/{tablename}"

        if direction == "staging":
            ddlstatement = f"""
            CREATE DATABASE IF NOT EXISTS {ctasdb};
            CREATE TABLE IF NOT EXISTS {ctasdb}.{tablename}
            AS SELECT * FROM '{deepsqltablepath}';
            """
        else:
            ddlstatement = f"""
            CREATE DATABASE IF NOT EXISTS {database};
            CREATE TABLE IF NOT EXISTS {database}.{tablename}
            AS SELECT * FROM {using}.'{sourcelocation}';
            """

        print(ddlstatement)

    return None


def deepclone_candidates(objdict,hivepath):
    candidates = []

    for obj in objdict:
        try:
            if obj['type'] == "TABLE" and obj['using'] == "delta":
                if "location" not in obj:
                    addlocation = ""
                    if obj['database'] == "default":
                        # default tables are written to metastore root
                        addlocation = f"{hivepath}{obj['table']}"
                    else:
                        addlocation = f"{hivepath}{obj['database']}.db/{obj['table']}"
                    
                    obj['location'] = addlocation
                
                if obj['location'][0:len(hivepath)] == hivepath:
                    #print(f"Candidate! {obj['table']}")
                    candidates.append(obj)

        except Exception as e:
            print(f"[ERROR] Probably not a delta table! {obj}")
            pass

    return candidates


def ctascopy_candidates(objdict,hivepath):
    candidates = []

    for obj in objdict:
        try:
            if obj['type'] == "TABLE" and obj['using'] != "delta":
                if "location" not in obj:
                    addlocation = ""
                    if obj['database'] == "default":
                        # default tables are written to metastore root
                        addlocation = f"{hivepath}/{obj['table']}"
                    else:
                        addlocation = f"{hivepath}/{obj['database']}.db/{obj['table']}"
                    
                    obj['location'] = addlocation
                
                if obj['location'][0:len(hivepath)] == hivepath:
                    print(f"Candidate! {obj['table']}")
                    candidates.append(obj)

        except Exception as e:
            print(f"[ERROR] Probably not a CTAS candidate! {obj}")
            pass

    return candidates


if __name__ == "__main__":
    myconfig = prog_config()
    myconfig.loadArgs()

    showDebug = True
    
    if showDebug:
        print(f"=== Configuration Summary ===")
        print(f"""
            Paths:
                Export: {myconfig.export_path}
                Hive: {myconfig.hive_path}
                Staging: {myconfig.staging_path}
            
            Display:
                Show Summary: {myconfig.show_summary}
                Show Details: {myconfig.show_details}
                Show DEEP: {myconfig.show_deep}
                Show CTAS: {myconfig.show_ctas}
        """)

    
    # extract details from ddl files
    allobjs = ddl_extract(myconfig.export_path)

    # process for deepclone and ctas candidates
    deepobjs = deepclone_candidates(allobjs, myconfig.hive_path)
    ctasobjs = ctascopy_candidates(allobjs, myconfig.hive_path)

    if myconfig.show_summary:
        print("=== Analysis Summary ===")
        print(f"""
        Total DDL statements: {len(allobjs)}
        Deep Clone candidates: {len(deepobjs)}
        CTAS Copy candidates: {len(ctasobjs)}
        """)

    if myconfig.show_details:
        print(f"\n\n=== Table Details ===")
        print(*allobjs, sep = "\n")

    if myconfig.show_deep:
        print(f"\n\n=== DEEP CLONE DDL Statements ===")

        print(f"\n-- Source -> Staging --")
        deepclone_build(deepobjs,migrate_prefix="staging_",direction="staging", stagingpath=myconfig.staging_path)

        print(f"\n-- Staging -> Target --")
        deepclone_build(deepobjs,migrate_prefix="staging_",direction="target", stagingpath=myconfig.staging_path)

    ## COMING SOON ##
    # if myconfig.show_ctas:
    #     print(f"\n\n=== CREATE TABLE AS (CTAS) DDL Statements ===")

    #     print(f"\n-- Source -> Staging --")
    #     ctascopy_build(ctasobjs,migrate_prefix="staging_",direction="staging", stagingpath=myconfig.staging_path)

    #     print(f"\n-- Staging -> Target --")
    #     ctascopy_build(ctasobjs,migrate_prefix="staging_",direction="target", stagingpath=myconfig.staging_path)

