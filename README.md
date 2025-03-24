# Bottom Up Accruals AWS Serverless Project

Accrued Revenue report required for finance, 
how many units has a customer consumed in a month or over a periods selected that we have not billed for 
(this should consider where a customer is not yet due to be billed).

https://jira.alintaenergy.com.au/browse/CB-6866

## Service Dependencies

The BUA process relies upon the version of workflow and meterdata matching what was in EARL when the snapshot was taken.

```Each month after the CORE release it is important to update the service version in matten```

```When updating the service version in matten it is important to update any variables that have changed as well```

```The project makefile target check-workflow-config-matten will check for workflow config differences between earl and matten```

```The project makefile target check-meterdata-config-matten will check for meterdata config differences between earl and matten```

```Note that most differences are acceptable since the config in matten is meant to be different to earl to support higher performance```

### ANSTEAD

| Accrual        | Snapshot                                                                    | Snapshot Date  | Workflow | Meterdata | Anstead | Notes                                           |
|----------------|-----------------------------------------------------------------------------|----------------|----------|-----------|---------|-------------------------------------------------|
| September 2023 | prod-data-2023-10-01-snapshot-shared-key                                    | 1st Oct 2023   | v25427   | v27560    | #24     | workflow with improved retry sequence key logic |
| October 2023   | prd-earl-1-sql-21-20-oct-31-2023-shared-key-encrypted                       | 1st Nov 2023   | v25427   | v27598    |         |
| November 2023  | prd-earl-1-sql-13-00-nov-30-2023-shared-key-encrypted                       | 1st Dec 2023   | v25427   | v27649    | #26     |
| December 2023  | prd-earl-1-sql-13-00-dec-31-2023-shared-key-encrypted                       | 1st Jan 2024   | v25427   | v27718    | #27     |
| December 2023  | tmp-prd-earl-1-sql-23-15-Jan-21-2024-11-24-Jan-22-2024-shared-key-encrypted | 21st Jan 2024  | v25433   | v27843    | #29     | disable keepalive in workflow                   |
| December 2023  | tst-anstead-29-bua-sql-20240101-20240123-20240125-095423                    | 21st Jan 2024  | v25427   | v27843    | #30     | testing rds oom during invoicing                |
| January 2024   | tmp-prd-earl-1-sql-13-00-Jan-31-2024-02-55-Feb-01-2024-shared-key-encrypted | 1st Feb 2024   | v25427   | v27843    | #32     |
| January 2024   | tst-anstead-32-bua-sql-20240201-20240201-20240203-002614                    | 1st Feb 2024   | v25427   | v27843    | #33     | testing rds oom during invoicing                |
| January 2024   | tmp-prd-earl-1-sql-13-00-Jan-31-2024-02-55-Feb-01-2024-shared-key-encrypted | 1st Feb 2024   | v25427   | v27843    | #35     | testing all config changes effects on all steps |
| January 2024   | tmp-prd-earl-1-sql-10-37-feb-20-2024-02-02-feb-21-2024-shared-key-encrypted | 21st Feb 2024  | v25427   | v27899    | #37     | test run before the end of the month            |
| Feb 2024       | tmp-prd-earl-1-sql-05-16-mar-25-2024-20-37-mar-25-2024-shared-key-encrypted | 26th Mar 2024  | v25399   | v27932    | #40     | test run before the end of the month            |
| March 2024     | tmp-prd-earl-1-sql-08-24-apr-23-2024-00-53-apr-24-2024-shared-key-encrypted | 24th Apr 2024  | v25399   | v27932    | #43     | test run before the end of the month            |
| April 2024     | tmp-prd-earl-1-sql-14-00-May-31-2024-20-24-Jun-03-2024-shared-key-encrypted | 3rd June 2024  | v25399   | v28011    | #45     | test run after the end of the month             |
| May 2024       | tmp-prd-5-6-24-shared-encrypted-key                                         | 26th June 2024 | v25399   | v28110    | #47     | test run before the end of the month            |
| May 2024       | tmp-prd-5-6-24-shared-encrypted-key                                         | 11th July 2024 | v25399   | v28110    | #48     | Utility profiles run for 3 years of data.       |
| June 2024      | tmp-prd-5-6-24-shared-encrypted-key                                         | 31st July 2024 | v25399   | v28129    | #49     | test run for period 01/05/2024 - 30/06/2024     |
| July 2024      | tmp-prd-earl-1-sql-23-50-aug-25-2024-13-49-aug-26-2024-shared-key-encrypted | 27th Aug 2024  | v25449   | v28148    | #52     | test run before the end of the month            |
| Aug 2024       | tmp-prd-earl-1-sql-01-13-sep-25-2024-18-13-sep-25-2024-shared-key-encrypted | 28th Sep 2024  | v25449   | v28167    | #56     | snapshot creation extremely slow. Prod limit 100|

### MATTEN

| Accrual        | Snapshot                                              | Snapshot Date | Workflow | Meterdata | Matten   | Runtime                                               |
|----------------|-------------------------------------------------------|---------------|----------|-----------|----------|-------------------------------------------------------|
| September 2023 | prod-data-2023-10-01-snapshot-shared-key              | 1st Oct 2023  | v25427   | v27560    | #19      |
| October 2023   | prd-earl-1-sql-21-20-oct-31-2023-shared-key-encrypted | 1st Nov 2023  | v25427   | v27598    | #13, #20 |
| November 2023  | prd-earl-1-sql-13-00-nov-30-2023-shared-key-encrypted | 1st Dec 2023  | v25427   | v27649    | #16      |
| December 2023  | prd-earl-1-sql-13-00-dec-31-2023-shared-key-encrypted | 1st Jan 2024  | v25427   | v27718    | #18      | 21:45:11                                              |
| January 2024   | prd-earl-1-sql-13-00-jan-31-2024-shared-key-encrypted | 1st Feb 2024  | v25427   | v27843    | #21      | 21:46:22                                              |
| February 2024  | prd-earl-1-sql-13-00-feb-29-2024-shared-key-encrypted | 1st Mar 2024  | v25427   | v27893    | #22      | 24:45:55                                              |
| March 2024     | prd-earl-1-sql-13-00-mar-31-2024-shared-key-encrypted | 1st Apr 2024  | v25427   | v27893    | #23      | 21:02:58                                              |
| April 2024     | prd-earl-1-sql-2024-04-30-14-13-shared-key-encrypted  | 1st May 2024  | v25427   | v27893    | #27      | 20:06:55 (Manual run using system generated snapshot) |
| May 2024       | prd-earl-1-sql-14-00-May-31-2024-shared-key-encrypted | 1st June 2024 | v25427   | v27995    | #28      | 20:35:06.686                                          |
| June 2024      | prd-earl-1-sql-14-00-jun-30-2024-shared-key-encrypted | 1st July 2024 | v25427   | v28110    | #29      | 23:01:50.672                                          |
| July 2024      | prd-earl-1-sql-14-00-jul-31-2024-shared-key-encrypted | 1st Aug 2024  | v25427   | v28135    | #30      | 26:08:30.699                                          |
| August 2024    | prd-earl-1-sql-14-00-aug-31-2024-shared-key-encrypted | 1st Sep 2024  | v25449   | v28148    | #31      | 20:56:08.375                                          |
| September 2024 | prd-earl-1-sql-14-00-sep-30-2024-shared-key-encrypted | 1st Oct 2024  | v25449   | v28167    | #32      | 2D:13H (3 redrives due to failures)                   |
| March 2025     | prd-earl-1-sql-13-00-feb-28-2025-shared-key-encrypted | 1st Mar 2025  | v25448   | v28350    |          |                                                       |


* Note: Runtime is the time when the stepfunction in earl that takes the snapshot starts until the time when the bua stepfunction in matten completed.
* Note: If the stepfunction fails then exclude any time when it was not running from the calculation.

## Monthly pre-run checks (after the CORE release each month)

#### Update version of services in ANSTEAD and MATTEN

1. Get the latest version of workflow and meterdata from EARL
2. Update the cluster project for matten and anstead with the same versions
3. Update the configuration in the matten EKS cluster with the same versions manually
4. Update the configuration in the anstead EKS cluster with the same versions manually

#### Update service configuration changes from EARL into ANSTEAD and MATTEN

1. Run the makefile target check-workflow-config-matten to check for new config for workflow
2. Update the cluster project for matten and anstead with any new config needed for workflow
3. Update the configuration in matten EKS cluster with any new config needed for workflow
4. Update the configuration in anstead EKS cluster with any new config needed for workflow
5. Run the makefile target check-meterdata-config-matten to check for new config for meterdata
6. Update the cluster project for matten and anstead with any new config needed for meterdata
7. Update the configuration in matten EKS cluster with any new config needed for meterdata 
8. Update the configuration in anstead EKS cluster with any new config needed for meterdata 

#### Validate EKS configuration in ANSTEAD and MATTEN

1. Execute the gitlab task anstead:90:check:eks to verify that the cluster works with the updated configuration
2. Execute the gitlab task matten:90:check:eks to verify that the cluster works with the updated configuration

#### Get latest EARL snapshot and test in ANSTEAD

```
Alinta platforms gitlab project to generate EARL snapshots for ANSTEAD
https://gitlab.com/alintaenergy/ops/utils/month-end-prod-data-workflow/-/pipelines
```

1. Execute the task anstead-rds-refresh in the ops/utils/month-end-prod-data-workflow gitlab project pipeline
2. Check the tst-anstead-bua step function has executed successfully in ANSTEAD once the above gitlab task has completed
3. Execute the anstead:99:rerun gitlab task in the bua-aws gitlab project to test the snapshot in ANSTEAD
4. Check the tst-anstead-bua step function has re-run succesfully.

#### Check the EARL monthly schedule
1. Check the EventBridge rule in EARL Prod-monthly-data-copy-Workflo-ScheduleRuleDA5BD877-mQj81xNFV30u will run at the right time (in local time zone)

#### Check the EARL stored procedure
1. Check the bua_prepare_export_data stored procedure in EARL Turkey_BLU
```
SHOW CREATE PROCEDURE bua_prepare_export_data;
```
Need to make sure the sql_mode for bua_prepare_export_data is "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
If STRICT_TRANS_TABLES exists in the sql_mode, use below command to remove it.
```
export MYSQL_HOST=<host>
export MYSQL_PORT=<por>
export MYSQL_USER=<db-user>
export file=conf/Procedures/R__bua_prepare_export_data.sql
cat "${file}" | mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" --user="${MYSQL_USER}" --init-command="SET SESSION SQL_MODE='REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI';" Turkey_BLU -p
```

## Manually executing the run

If the last run needs to be repeated in a new RDS instance then use the gitlab task ```rerun``` to achieve this.

If particular steps need to be run with the current RDS instance then use the relevant gitlab task.

## Deleting the RDS instance

The best way to delete an RDS instance from a prior run is to delete the cloudformation stack that created it.

For example: 
```aws cloudformation delete-stack --stack-name prd-matten-16-bua-sql``` 


## Execution and Monitoring

### BEWARE, WARNING

**IT IS NOT A GOOD IDEA TO RUN THE PROFILING STEP FUNCTION TWICE FOR THE SAME RUN DATE**

The reason is it takes a while to delete the old records and the lambda often times out trying.
If you need to then manually delete the old records first and then run it.

<details>
<summary>Run this SQL to manually delete the old records before rerun UtilityProfile step</summary>


```
TRUNCATE TABLE UtilityProfile;
TRUNCATE TABLE UtilityProfileVariance;
TRUNCATE TABLE UtilityProfileSummary;
```

</details>

**IT IS IMPORTANT THAT THE SAME RUN DATE IS USED FOR ALL STEP FUNCTIONS**

If you do not then chances are the correct data will not be found by subsequent step functions.
If you need to re-run steps after an automated run fails, 
then ensure you use the same run date for the manual steps as was used for the automated run.

**DO NOT RUN TESTS IN ANSTEAD ON THE 1st OF THE MONTH**

On the first of the month a snapshot of EARL is provided to MATTEN and automatically triggers the monthly run.
The same snapshot is then anonymised in EARL and then provided to ANSTEAD where it triggers updating the parameter store ready for use.
The anonymisation takes about 15 hours, so this occurs around 3-4pm on the 1st of the month in ANSTEAD.
Therefore, do not run the step functions on the 1st of the month in ANSTEAD to avoid conflicts with this automated process.


## Architecture

### EARL Pipeline

```mermaid
graph TD;
EARL-1stOfMonth-Midnight-Schedule --> EARL-Encrypted-Snapshot;
EARL-Encrypted-Snapshot --> EARL-ReEncrypt-Snapshot;
EARL-ReEncrypt-Snapshot --> EARL-Shared-Snapshot;
EARL-Shared-Snapshot --> EARL-Publish-To-Topic;
```

### MATTEN Pipeline

```mermaid
graph TD;
EARL-Publish-To-Topic --> MATTEN-Notify-SQS-Queue;
MATTEN-Notify-SQS-Queue --> MATTEN-Notify-Lambda;
MATTEN-Notify-Lambda --> MATTEN-BUA;
EARL-Shared-Snapshot --> MATTEN-Restore;
MATTEN-BUA --> MATTEN-ScaleDown-0;
MATTEN-ScaleDown-0 --> MATTEN-Restore;
MATTEN-Restore --> MATTEN-EmptyQueues;
MATTEN-EmptyQueues --> MATTEN-CleanWorkflows;
MATTEN-CleanWorkflows --> MATTEN-CleanInvoiceAttribute;
MATTEN-CleanInvoiceAttribute --> MATTEN-CleanUtilityProfile;
MATTEN-CleanUtilityProfile --> MATTEN-ScaleUpWorkflow;
MATTEN-ScaleUpWorkflow --> MATTEN-WarmStatistics;
MATTEN-WarmStatistics --> MATTEN-WarmIndexes;
MATTEN-WarmIndexes --> MATTEN-ScaleDown-1;
MATTEN-ScaleDown-1 --> MATTEN-UtilityProfiles;
MATTEN-UtilityProfiles --> MATTEN-Segments;
MATTEN-Segments --> MATTEN-Snapshot-1;
MATTEN-Snapshot-1 --> MATTEN-Microscalar;
MATTEN-Microscalar --> MATTEN-BasicReads;
MATTEN-BasicReads --> MATTEN-ScaleUpMeterdata;
MATTEN-ScaleUpMeterdata --> MATTEN-GenerateNEM12;
MATTEN-GenerateNEM12 --> MATTEN-RestartMeterdata;
MATTEN-RestartMeterdata --> MATTEN-InvoiceRuns;
MATTEN-InvoiceRuns --> MATTEN-ILIExceptions;
MATTEN-ILIExceptions --> MATTEN-ScaleDown-2;
MATTEN-ScaleDown-2 --> MATTEN-Prepare;
MATTEN-Prepare --> MATTEN-Snapshot-2;
MATTEN-Snapshot-2 --> MATTEN-Export;
MATTEN-Export --> MATTEN-ProfileValidation;
MATTEN-ProfileValidation --> MATTEN-DumpErrors;
MATTEN-DumpErrors --> MATTEN-SwitchBastion;
MATTEN-Destroy;
```

## Stored Procedures

| Stored Procedure           | Purpose                                                                            |
|----------------------------|------------------------------------------------------------------------------------|
| bua_create_basic_read      | Create missing basic read records for an account                                   |
| bua_create_invoice_scalar  | Create invoice scalar records for an account                                       |
| bua_create_macro_profile   | Create macro scalar records for an account                                         |
| bua_dates_to_check         | List the dates to check for the period being calculated                            |
| bua_mark_segment_jurisdiction_entries | Mark invalid segment jurisdiction entries                                          |
| bua_fill_marked_segment_jurisdiction_entries | Fill marked interval profiles with equivalent records                              |
| import_gas_volumes_profile_creation | Create gas profiles                                                                |
| bua_initiate               | Initiate calculations from SQL using workflow (not used)                           |
| bua_initiate_invoice_runs  | Initiate all invoice runs                                                          |
| bua_list_profile_registers | Get a list of all registers that are used for profile generation                   |
| bua_list_profile_nmis      | Get a list of all NMIs that require NEM12 file generation                          |
| bua_list_unbilled_accounts | Get a list of accounts that are unbilled for some period                           |
| bua_list_all_accounts      | Get a list of all accounts open at some time during the period                     |
| bua_list_missing_periods   | List the missing periods                                                           |
| bua_prepare_export_data    | Populate export tables with data for an account                                    |
| core_warm_database_statistics | Trigger the execution of statistics on each table and partition (uses workflow) |
| core_warm_database_indexes    | Trigger the execution of index warming for each index (uses workflow)           |
| core_warm_table               | Analyse a table                                                                    |
| core_warm_partition           | Analyze a partition                                                                |
| core_warm_index               | Warm an index                                                                      |

## Lambda functions

| Function                   | Purpose                                                                          |
|----------------------------|----------------------------------------------------------------------------------|
| lambda-bua-controller-fast | Controls execution of the BUA process (3 minute timeout)                         |
| lambda-bua-controller-slow | Controls execution of the BUA process (15 minute timeout)                        |
| lambda-bua-next            | Controls next step in SQS processing control                                     |
| lambda-bua-notify          | Trigger the BUA step function based on a notification from a topic               |
| lambda-bua-site-basic      | Low concurrency SQS driven missing basic reads execution (32 concurrent)         |
| lambda-bua-site-data       | High concurrency SQS driven utility profile processing (800 concurrent)          |
| lambda-bua-site-export     | Low concurrency SQS driven export of BUA data (32 concurrent)                    |
| lambda-bua-site-initiate   | SQS driven site data processing initiation                                       |
| lambda-bua-site-mscalar    | Low concurrency SQS driven microscalar execution (32 concurrent)                 |
| lambda-bua-site-nem12      | Low concurrency SQS driven NEM12 generation (32 concurrent)                      |
| lambda-bua-site-prepare    | Low concurrency SQS driven prepare export data (32 concurrent)                   |
| lambda-bua-site-segment    | Low concurrency SQS driven interval profile segment calculations (32 concurrent) |

## State Machines

| State machine              | Purpose                                                    | Step Name             |
|----------------------------|------------------------------------------------------------|-----------------------|
| bua                        | Controller machine                                         | DoNothing             |
| bua-basicreads             | Calculate missing basic reads                              | BasicReads            |
| bua-clean-invoiceattribute | Clean the invoiceattribute table                           | CleanInvoiceAttribute |
| bua-clean-utilityprofile   | Clean the utilityprofile table                             | CleanUtilityProfile   |
| bua-clean-workflows        | Clean the workflowinstance table                           | CleanWorkflows        |
| bua-destroy                | Destroy the BUA RDS instance                               | Destroy               |
| bua-dump-errors            | Dump messages in failure and dlq SQS queues to S3          | DumpErrors            |
| bua-empty-queues           | Purge failure and dlqs from SQS                            | EmptyQueues           |
| bua-export                 | Export BUA data to S3                                      | Export                |
| bua-ili-exceptions         | Run ili exceptions filters                                 | ILIExceptions         |
| bua-invoiceruns            | Execute all invoice runs                                   | InvoiceRuns           |
| bua-microscalar            | Calculate microscalar values                               | Microscalar           |
| bua-nem12                  | Generate missing NEM12 files                               | GenerateNEM12         |
| bua-prepare                | Prepare data for export to S3                              | Prepare               |
| bua-profile-validation     | Perform utility profile validation                         | ProfileValidation     |
| bua-reset-basicreads       | Reset any previously generated basic reads                 | ResetBasicReads       |
| bua-reset-nem12            | Reset generated NEM12 files                                | ResetNEM12            |
| bua-restart-meterdata      | Restart the workflow and meterdata pods                    | RestartMeterdata      |
| bua-restore                | Restore a RDS snapshot                                     | Restore               |
| bua-scaledown              | Scale down nodegroup and replicas                          | ScaleDown             |
| bua-scaleup-meterdata      | Scale up nodegroup and replicas for workflow and meterdata | ScaleUpMeterdata      |
| bua-scaleup-workflow       | Scale up nodegroup and replicas for workflow to execute    | ScaleUpWorkflow       |
| bua-segments               | Calculate profile segments                                 | Segments              |
| bua-snapshot               | Take a snapshot of the RDS instance                        | Snapshot              |
| bua-switch-bastion         | Switch the Route53 entry for the bastion RDS port          | SwitchBastion         |
| bua-utility-profiles       | Extract DDB data and calculate utility profiles            | UtilityProfiles       |
| bua-warm-indexes           | Warm the indexes of specific tables                        | WarmIndexes           | 
| bua-warm-statistics         | Warm the statistics of all tables                         | WarmStatistics        |
| bua-warming                | Force read from S3 to EBS for an RDS instance              | Warming               |

## Step Functions

### bua-restore

*bua-restore* is used to create an RDS instance from a production snapshot.

#### Prerequisites

None

#### Dependencies

| Type      | Name              |
|-----------|-------------------|
| Parameter | cluster_name      |
| Parameter | domain            |
| Parameter | hosted_zone_id    |
| Parameter | instance_class    |
| Parameter | instance_type     |
| Parameter | mysql_version     |
| Parameter | node_group_name   |
| Parameter | params_id         |
| Parameter | rds_dns_alias     |
| Parameter | rdssecret         |
| Parameter | schema            |
| Parameter | snapshot_arn      |
| Parameter | source_account_id |
| Parameter | sqlsecret         |
| Parameter | suffix            |
| Parameter | update_id         |

#### Steps

1. Copy snapshot from EARL to MATTEN to use for restoring a new RDS instance.
2. Execute Cloudformation template to create an RDS instance from a snapshot.
2. Reset the core_admin password of the new RDS instance.
3. Disable all workflow schedules
4. Set user passwords (for workflow, meterdata, and lambda)
6. Calculate statistics for AggregatedRead
7. Scale down any workflow or meterdata pods
8. Set the Route53 entry to point to the new RDS instance
9. Set the bua account id GlobalSetting value to this account



### bua-scaleup-workflow

*bua-scaleup-workflow* is used to scale the EKS cluster to 1 node and workflow pods to 1 replicas.

#### Prerequisites

None

#### Dependencies

| Type      | Name              |
|-----------|-------------------|
| Parameter | cluster_name      |

#### Steps

1. scale EKS node group to 1 nodes
2. scale workflow to 1 replicas



### bua-warm-statistics

*bua-warm-statistics* is used to drag in as many blocks from S3 to EBS as possible on the newly created database.

#### Prerequisites

| Step        |
|-------------|
| bua-restore |

#### Dependencies

| Type      | Name                            |
|-----------|---------------------------------|
| Service   | workflow-runner                 |
| Procedure | core_warm_database_statistics   |

#### Steps

1. execute core_warm_database_statistics
2. wait for workflows to complete



### bua-warm-indexes

*bua-warm-indexes* is used to drag in as many blocks from S3 to EBS as possible on the newly created database.

#### Prerequisites

| Step                |
|---------------------|
| bua-warm-statistics |

#### Dependencies

| Type    | Name            |
|---------|-----------------|
| Service | workflow-runner |

#### Steps

3. create EventLog records to warm indexes for select tables
4. wait for workflows to complete



### bua-utility-profiles

*bua-utility-profiles* is used to extract the data from ddb and calculate utility profiles for interval electricity sites.

#### Prerequisites

| Step                |
|---------------------|
| bua-warm-statistics |
| bua-warm-indexes    |

#### Dependencies

| Type      | Name                       |
|-----------|----------------------------|
| Procedure | bua_list_profile_registers |

#### Steps

1. Warm the MeterRegister table
2. Warm the MarketPayloadMapping table
3. Warm the Meter table
4. Warm the Utility table
5. Warm the UtilityDetail table
6. Warm the UtilityNetwork table
7. Warm the UtilityTni table
8. Warm the Jurisdiction table
9. Warm the ServiceType table
10. Warm the MeterRegister table
11. Clean the UtilityProfile table
12. initiate extract interval reads from DDB to UtilityProfile
13. Call bua_list_profile_registers
13. wait for SQS queues to empty

#### Logging

BUAControl table records progress of the controller

UtilityProfileLog table records progress of the utility profile processing



### bua-segments

*bua-segments* is used to calculate the segment profiles for interval electricity sites.

#### Prerequisites

| Step                 |
|----------------------|
| bua-utility-profiles |

#### Dependencies

| Type      | Name                                         |
|-----------|----------------------------------------------|
| Procedure | bua_dates_to_check                           |
| Procedure | bua_mark_segment_jurisdiction_entries        |
| Procedure | bua_fill_marked_segment_jurisdiction_entries |
| Procedure | bua_create_macro_profile                     |

#### Steps

1. initiate calculation of SegmentJurisdictionAvgExclEst
2. wait for SQS queues to empty
3. initiate SegmentJurisdictionCheck to find invalid segments
4. wait for SQS queues to empty
5. initiate SegmentJurisdictionFix to fix invalid segments
6. wait for SQS queues to empty
9. execute bua_create_macro_profile

#### Logging

BUAControl table records progress of the controller

UtilityProfileLog table records progress of the utility profile processing



### bua-microscalar

*bua-microscalar* is used to calculate the microscalars for each account.

#### Prerequisites

| Step         |
|--------------|
| bua-segments |

#### Dependencies

| Type      | Name                        |
|-----------|-----------------------------|
| Procedure | bua_prep_unbilled_accounts  |
| Procedure | bua_create_invoice_scalar   |

#### Steps

1. initiate MicroScalar to calculate micro scalar records
2. wait for SQS queues to empty



### bua-reset-basicreads

*bua-reset-basicreads* is used to clear out any generated basic reads.

#### Prerequisites

None

#### Dependencies

| Type      | Name                       |
|-----------|----------------------------|
| Procedure | bua_prep_unbilled_accounts |
| Procedure | bua_reset_basic_read       |

#### Steps

1. initiate ResetBasicRead to reset any prior calculated basic reads
2. wait for SQS queues to empty
3. check BUAControl table for any failures



### bua-basicreads

*bua-basicreads* is used to generate any missing basic reads.

#### Prerequisites

| Step            |
|-----------------|
| bua-microscalar |

#### Dependencies

| Type      | Name                       |
|-----------|----------------------------|
| Procedure | bua_prep_unbilled_accounts |
| Procedure | bua_create_basic_read      |

#### Steps

1. initiate BasicRead to create missing basic reads
2. wait for SQS queues to empty
3. check BUAControl table for any failures



### bua-scaleup-meterdata

*bua-scaleup-meterdata* is used to scale EKS to 10 nodes and meterdata to 8 replicas.

#### Prerequisites

None

#### Dependencies

| Type      | Name              |
|-----------|-------------------|
| Parameter | cluster_name      |

#### Steps

1. scale EKS node group to 10 nodes
2. scale workflow to 1 replicas
2. scale meterdata to 8 replicas



### bua-nem12

*bua-nem12* is used to generate and load missing electricity interval reads.

#### Summary

1. Use the bua_initiate action to get a list of NMI and date ranges to process
2. For each NMI and date range submit a message to a queue.
3. Use a cluster of 32 nem12 lambdas to consume the messages on the queue.
4. For each message query the database for missing periods in AggregatedRead. 
5. Construct a NEM12 file for the missing periods and submit it to the standard NEM12 file processing S3 bucket.
6. Standard NEM12 file processing loads the NEM12 file into DDB and triggers import and aggregation in CORE using workflow.

#### Prerequisites

| Step           |
|----------------|
| bua-segments   |

#### Dependencies

| Type      | Name                  |
|-----------|-----------------------|
| Procedure | bua_prep_profile_nmis |
| Service   | workflow-runner       |
| Service   | meterdata             |

#### Steps

1. initiate NEM12 generation for missing electricity interval reads
2. wait for SQS queues to empty
3. wait for workflow instances to complete
4. reschedule any failed workflow instances
5. wait for rescheduled workflow instances to complete
6. check for any remaining failed workflow instances



### bua-invoiceruns

*bua-invoiceruns* is used to execute invoicing for all accounts.

#### Prerequisites

| Step            |
|-----------------|
| bua-segments    |
| bua-microscalar |
| bua-basicreads  |

#### Dependencies

| Type      | Name                      |
|-----------|---------------------------|
| Procedure | bua_initiate_invoice_runs |
| Service   | workflow-runner           |
| Service   | meterdata                 |

#### Steps

1. set priorities of workflows appropriately
2. initiate invoice run batches
3. wait for all workflow schedules to execute
4. wait for GENERATE_ILI to complete
5. wait for RUN_INVOICE_BATCH to complete
6. resubmit failed RUN_INVOICE_BATCH
7. wait for resubmitted RUN_INVOICE_BATCH to complete
8. check for any remaining failed RUN_INVOICE_BATCH
9. wait for INVOICEGEN to complete
10. resubmit failed INVOICEGEN
11. wait for resubmitted INVOICEGEN to complete
12. check for any remaining failed INVOICEGEN



### bua-scaledown

*bua-scaledown* is used to scale meterdata and workflow to 0 replicas and EKS to 0 nodes.

#### Prerequisites

None

#### Dependencies

| Type      | Name              |
|-----------|-------------------|
| Parameter | cluster_name      |

#### Steps

1. scale EKS node group to 0 nodes
2. scale workflow and meterdata to 0 replicas



### bua-prepare

*bua-prepare* is used to prepare the BUA data for export to S3.

#### Prerequisites

| Step             |
|------------------|
| bua-invoiceruns  |

#### Dependencies

| Type      | Name                    |
|-----------|-------------------------|
| Procedure | bua_prep_all_accounts   |
| Procedure | bua_prepare_export_data |

#### Steps

1. Truncate BUAAccountSummary and InvoiceLineItemMonthly
2. Remove any old S3 files from prd-matten-s3-bua/export/csv/{{run_date}}/
3. Initiate preparation of data to export
4. Wait for SQS queues to empty



### bua-export

*bua-export* is used to export the BUA data to S3.

#### Prerequisites

| Step        |
|-------------|
| bua-prepare |

#### Dependencies

None

#### Steps

1. Export BUAAccountSummary as CSV to prd-matten-s3-bua/export/csv/{{run_date}}/
2. Export InvoiceLineItemMonthly as CSV to prd-matten-s3-bua/export/csv/{{run_date}}/
3. Wait for SQS queues to empty
4. Copy S3 objects from prd-matten-s3-bua/export/csv/{{run_date}}/ to prd-matten-s3-rw-integration/ADH/OUTBOUND/BUA/



### bua-destroy

*bua-destroy* is used to destroy the BUA RDS instance.

#### Prerequisites

None

#### Dependencies

None

#### Steps

1. Delete the cloudformation stack (e.g. prd-matten-14-bua-sql)



# Utility Profile Step - Temporary Fix Guide

For each month's BUA run, we keep monitoring the Matten AWS Step Functions. If the `UtilityProfile` step fails due to exceeding the AWS Lambda 15-minute limitation, we will see the `UtilityProfile` step function in red. In such cases, we need to apply a temporary fix and then redrive the main BUA step function.

## Steps to Apply Temporary Fix

1. **Clean History Data**
   - Execute the necessary SQL commands to clean history data.
   
<details>
<summary>SQL truncate tables</summary>


```
TRUNCATE TABLE UtilityProfile;
TRUNCATE TABLE UtilityProfileVariance;
TRUNCATE TABLE UtilityProfileSummary;
```

</details>

2. **Create and Populate Temporary Table**
   - Manually run SQL to create a temporary table.
   - Manually run SQL to create a stored procedure.
   - Call the stored procedure to populate this temporary table.

<details>
<summary>Create bua_list_profile_registers_temp table</summary>


```
CREATE TABLE "bua_list_profile_registers_temp" (
  "nmi" varchar(15) DEFAULT NULL,
  "res_bus" varchar(3) DEFAULT NULL,
  "jurisdiction" varchar(50) DEFAULT NULL,
  "nmi_suffix" varchar(10) DEFAULT NULL,
  "stream_type" varchar(7) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  "tni" varchar(50) DEFAULT NULL
)
```

</details>

<details>
<summary>Create stored proc</summary>


```
DELIMITER $$
CREATE PROCEDURE bua_list_profile_registers_ruibin(
IN in_start_inclusive DATE,
IN in_end_exclusive DATE,
IN in_today DATE,
IN in_run_date DATETIME)
COMMENT 'List the registers that are to be included in profile segment generation'
BEGIN
    DECLARE vi_run_date DATETIME DEFAULT COALESCE(in_run_date, CAST(CAST(DATE_SUB(NOW(), INTERVAL DAYOFMONTH(NOW())-1 DAY) AS DATE) AS DATETIME));
    DECLARE vi_today DATE DEFAULT COALESCE(in_today, DATE_SUB(CAST(vi_run_date AS DATE), INTERVAL DAYOFMONTH(vi_run_date)-1 DAY));
    DECLARE vi_month_start DATE DEFAULT DATE_SUB(vi_today, INTERVAL DAYOFMONTH(vi_today)-1 DAY);
    DECLARE vi_year_ago DATE DEFAULT DATE_SUB(vi_month_start, INTERVAL 1 YEAR);
    DECLARE vi_start_inclusive DATE DEFAULT COALESCE(in_start_inclusive, vi_year_ago);
    DECLARE vi_end_exclusive DATE DEFAULT COALESCE(in_end_exclusive, vi_month_start);

        DELETE FROM bua_list_profile_registers_temp;
        INSERT INTO bua_list_profile_registers_temp (nmi, res_bus, jurisdiction, nmi_suffix, stream_type, tni)
    SELECT
        ut.identifier AS 'nmi',
        SUBSTR(COALESCE(ut.cust_class_code, 'RESIDENTIAL'),1,3) AS 'res_bus',
        ju.name AS 'jurisdiction',
        mr.suffix_id AS 'nmi_suffix',
        CASE
            WHEN COALESCE(mp.value, 'NO') = 'YES' THEN 'CONTROL'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'A' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'B' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'C' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'J' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'K' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'L' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'N' AND mr.direction_indicator = 'I' THEN 'SOLAR'
            WHEN SUBSTR(mr.suffix_id,1,1) = 'X' AND mr.direction_indicator = 'I' THEN 'SOLAR'
            ELSE 'PRIMARY'
            END AS 'stream_type',
        MAX(IF(ud.utility_status='A',tn.name,'')) AS 'tni'
    FROM MeterRegister mr
    LEFT JOIN MarketPayloadMapping mp ON mp.market_value = mr.controlled_load AND mp.market_tag = 'ControlledLoad'
    JOIN Meter mt ON mr.meter_id = mt.id
    JOIN Utility ut ON mt.utility_id = ut.id
    JOIN UtilityDetail ud ON ud.utility_id = ut.id
    JOIN UtilityNetwork un ON ut.utility_network_id = un.id
    JOIN UtilityTni tn ON ud.utility_tni_id = tn.id
    JOIN Jurisdiction ju ON ju.id = un.jurisdiction_id
    JOIN ServiceType st ON un.service_type_id = st.id AND st.name = 'ELECTRICITY'
    JOIN MeterRegisterDetail md ON md.meter_register_id = mr.id AND md.status = 'C' AND md.start_date < vi_end_exclusive AND COALESCE(md.end_date,NOW()) >= vi_start_inclusive
    WHERE (
        mt.meter_installation_type LIKE 'COMMS%'
        OR mt.meter_installation_type LIKE 'MR%'
    )
    AND LENGTH(ut.identifier) = 10
    GROUP BY ut.identifier, ut.cust_class_code, ju.name, mr.suffix_id, mp.value, mr.direction_indicator
    ORDER BY 1;
END;$$
DELIMITER ;
```

</details>

<details>
<summary>Populate bua_list_profile_registers_temp table</summary>


```
-- in_start_inclusive,     in_end_exclusive, in_today,   in_run_date
-- 2024-05-01 (last year), 2025-05-01,       2025-05-01, 2025-05-01

CALL bua_list_profile_registers_ruibin('2024-05-01', '2025-05-01', '2025-05-01', '2025-05-01');
```

</details>


3. **Deploy Hot-Fix Branch**
   - Deploy the hot-fix branch into the Matten environment. 
   - Ensure that the hot-fix branch is **not** merged into the master branch.
   - [hot-fix merge request](https://gitlab.com/alintaenergy/serverless/bua-aws/-/merge_requests/166/diffs)

4. **Redrive Main BUA Step Function**
   - After the temporary table is populated and the hot-fix branch is deployed, **redrive** the main BUA step function.

5. **Monitor the Run**
   - Continuously monitor the run until it is finished in Matten. Typically, the run starts at 0:00 on the 1st of the month and finishes around 6:00 am on the 2nd of the month.

6. **Check S3 Bucket**
   - Verify that the S3 bucket contains all the expected CSV files after the BUA step function is complete.
   - s3://prd-matten-s3-rw-integration/ADH/OUTBOUND/BUA

7. **Summarize the BUA Run**
   - Manually run an SQL query in Matten to get the summary of the BUA run.
   - Share the summary with the stakeholders:
     - "Martin, Yasna" <Yasna.Martin@alintaenergy.com.au>; 
     - "Seebacher, Anais" <Anais.Seebacher@alintaenergy.com.au>;
     - "Davis, Andrew" <Andrew.Davis@alintaenergy.com.au>; 
     - "Donelson, Melanie" <Melanie.Donelson@alintaenergy.com.au>; 
     - "Firth, Michael" <Michael.Firth@alintaenergy.com.au>; 
     - "Heffernan, Heath" <Heath.Heffernan@alintaenergy.com.au>; 
     - "Mukherjee, Jaya" <Jaya.Mukherjee@alintaenergy.com.au>; 
     - "Nickolas Kardamitsis" <Nickolas.Kardamitsis@tally-group.com>; 
     - "Ruibin Chen" <Ruibin.Chen@tally-group.com>;

<details>
<summary>SQL for summary and export into CSV</summary>


```
CREATE TEMPORARY TABLE tmp_accrual_export AS
SELECT ili_state,
       discount_state,
       feature_type_name,
       j.name state,
       st.name fuel,
       jas.name segment,
       ilim.year_mnth,
       time_class_name,
       SUM(ilim.net_amount) net_amount,
       SUM(IF(ilim.plan_item_type_name = 'USAGE_RETAIL' ,ilim.quantity, 0)) volume,
       SUM(IF(ilim.plan_item_type_name = 'DAILY_RETAIL',ilim.quantity, 0)) days,
       COUNT(distinct a.id) accounts,
       SUM(ilim.discount_net_propensity) discount_net_propensity,
       SUM(ilim.discount_net) discount_net,
       SUM(ilim.net_amount- ilim.discount_net_propensity) net_amount_less_discount
FROM InvoiceLineItemMonthly ilim
JOIN Account a ON a.id = ilim.account_id
JOIN ServiceType st ON st.id = a.service_type_id
JOIN JournalAccountSegment jas ON a.journal_segment_id = jas.id
JOIN AccountUtility au ON au.account_id = a.id
JOIN Utility u ON u.id = au.utility_id
JOIN Jurisdiction j ON j.id = u.jurisdiction_id
GROUP BY ili_state, discount_state,
      feature_type_name, j.name, st.name, jas.name, ilim.year_mnth, time_class_name;
      
SELECT *
FROM tmp_accrual_export;

```

</details>

8. **Stop Matten RDS Instances**
   - After receiving confirmation from stakeholders or after one week, stop the Matten RDS instances to save on budget.


## Notes

- Always ensure that the hot-fix branch is deployed correctly and that all temporary tables are populated before redriving the main BUA step function.
- Keep close communication with stakeholders to confirm the run's success and address any issues promptly.

