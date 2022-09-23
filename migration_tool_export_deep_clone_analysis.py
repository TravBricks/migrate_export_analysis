from ast import Constant
import glob
import re #,fileinput,os

hivepath="dbfs:/user/hive/metastore/"

#What path are we analyzing
ddlpath="/Users/travis.longwell/Git/migrate/azure_logs/M20220916104521/metastore/*/*"

#Where will the generated DEEP CLONE commands write to
stagingpath="/mnt/staging/deepclone/"


copydirection=["staging","target"][0]

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

def deepcopy_build(buildobjs,migrate_prefix="staging_",direction="staging", stagingpath=""):
    

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


def deepcopy_candidates(objdict):
    candidates = []

    for obj in objdict:
        try:
            if obj['type'] == "TABLE" and obj['using'] == "delta":
                if "location" not in obj:
                    addlocation = ""
                    if obj['database'] == "default":
                        # default tables are written to metastore root
                        addlocation = f"dbfs:/user/hive/metastore/{obj['table']}"
                    else:
                        addlocation = f"dbfs:/user/hive/metastore/{obj['database']}.db/{obj['table']}"
                    
                    obj['location'] = addlocation
                
                if obj['location'][0:len(hivepath)] == hivepath:
                    #print(f"Candidate! {obj['table']}")
                    candidates.append(obj)

        except Exception as e:
            print(f"[ERROR] Probably not a delta table! {obj}")
            pass

    return candidates


if __name__ == "__main__":

    # What are we scanning (Migrate export results)
    ddlpath="/Users/travis.longwell/Git/migrate/azure_logs/M20220916104521/metastore/*/*"
    stagingpath="/mnt/staging/deepclone/"
    
    # extract details from ddl files
    allobjs = ddl_extract(ddlpath)

    # whic are deep clone candidates
    deepobjs = deepcopy_candidates(allobjs)

    print(f"""
    === SUMMARY ===
    Total DDL statements: {len(allobjs)}
    Deep Clone candidates: {len(deepobjs)}
    """)

    print(f"""\n\n
    === DEEP CLONE DDL Statements ===
    """)

    deepcopy_build(deepobjs,migrate_prefix="staging_",direction=copydirection, stagingpath=stagingpath)
