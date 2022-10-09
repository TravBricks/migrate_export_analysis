# Databricks Migration Tool Export Analysis

## Purpose
There is a gap in the Databricks Migration tool when it comes to copying data between workspace `dbfs:/user/hive/warehouse` locations. This tool provides a path to use the Migration tool to inform the data copy process and identify potential issues with legacy or non-standard table DDL.

## How it works
The Databricks Migrate tool generates an output of all objects within a Databricks workspace. This program focuses on the metastore output which consists of folders and files where folders represent databases and files are table/view DDL statements.

## Process
1. Create cloud object storage that is mounted to the source and target workspaces with the same path (e.g. /mnt/migration)
1. Run Databricks Migrate tool to export the metastore
1. Run this program targeting the resultant metastore folder
1. Review output summary.txt and legacyissues.txt
1. Source -> Staging: 
    1. Copy __source_env_*.txt_ commands into a notbooks in source environment
    1. Run notebook(s) deep, ctas, then cleanup
1. Staging -> Target: 
    1. Copy __target_env_*.txt_ commands into a notbooks in target environment
    1. Run notebook(s) deep, ctas, then cleanup


## Usage
```
usage: migration_tool_export_analysis.py [-h] [--migrate_metastore_export MIGRATE_METASTORE_EXPORT] [-p SESSIONIDPATH]
                                         [--hive_path HIVE_PATH] [--staging_path STAGING_PATH] [--output_path OUTPUT_PATH]
                                         [--details_format {csv,json}] [--show_all] [--show_summary] [--show_details] [--show_issues]
                                         [--show_deep] [--show_ctas] [--show_cleanup] [--show_config]

Analyze migration tool exported metastore table/view DDL statements and in support of table data migration tasks.

optional arguments:
  -h, --help            show this help message and exit
  --migrate_metastore_export MIGRATE_METASTORE_EXPORT
                        Path of migration tool export (note:will recurse)
  -p SESSIONIDPATH, --sessionidpath SESSIONIDPATH
                        SESSION_ID File Path, interchangeable with --migrate_metastore_export
  --hive_path HIVE_PATH
                        The dbfs path where default managed databases and tables are written (should end with a '/') (default:
                        /user/hive/warehouse/)
  --staging_path STAGING_PATH
                        The absolute mount path of the staging bucket (default: /mnt/lnd/devmetastore_migration/)
  --output_path OUTPUT_PATH
                        Path to outputfile location
  --details_format {csv,json}
                        Define detail output format (default: json)
  --show_all            Include all show commands: summary, details, issues, deep, ctas and cleanup)
  --show_summary        Provides a summary of our analysis
  --show_details        Outputs a full list of tables analyzed
  --show_issues         Outputs a list of tables with non-standard using/format and other checks
  --show_deep           Outputs DDL DEEP CLONE statements
  --show_ctas           Outputs CTAS Table Copy statements
  --show_cleanup        Outputs environment cleanup commands
  --show_config         Show configuation details
```

## Examples
Run with full output and functionality
```
python3 ./migration_tool_export_analysis.py \
    --migrate_metastore_export "~/migration/M20220916104521/metastore_export/metastore/" \
    --staging_path "/mnt/migration/" \
    --show_all
```

Run with summary and issues
```
python3 ./migration_tool_export_analysis.py \
    --migrate_metastore_export "~/migration/M20220916104521/metastore_export/metastore/" \
    --staging_path "/mnt/migration/" \
    --show_summary --show_issues
```

## Output Files
This script will generate a number of output files to support the analysis and process.

### General
| Filename | Description |
| --- | ----------- |
| config.txt | Details about the configured analysis run |
| summary.txt | Details about how many tables/views were analyzed |
| legacyissues.txt | Older tables may contain DDL statements with currently unsupported __USING__ formats |

### Source Environment
| Filename | Description |
| --- | ----------- |
| source_env_deep.txt | __DEEP CLONE__ commands for copying source to staging for __DELTA__ tables that are located in managed __dbfs://user/hive/warehouse__ |
| source_env_ctas.txt | __CTAS__ commands for copying source to staging for non-__DELTA__ tables that are located in managed __dbfs://user/hive/warehouse__ |
| source_env_cleanup.txt | Clean-up staging database and table objects for source environement |

### Target Environment
| Filename | Description |
| --- | ----------- |
| target_env_deep.txt | __DEEP CLONE__ commands for copying staging to targetf or __DELTA__ tables that are located in managed __dbfs://user/hive/warehouse__ |
| target_env_ctas.txt | __CTAS__ commands for copying staging to target for non-__DELTA__ tables that are located in managed __dbfs://user/hive/warehouse__ |
| target_env_cleanup.txt | Clean-up staging database and table objects for source environement |