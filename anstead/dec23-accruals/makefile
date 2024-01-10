PROFILE=anstead
PREFIX=tst-anstead
ADH_BUCKET_NAME=tst-anstead-s3-rw-integration
END_EXCLUSIVE=2024-01-01
END_INCLUSIVE=2023-12-31
RUN_DATE=2024-01-10
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:760694178318:snapshot:prd-sql-2023-12-31-anstead-shared-key-encrypted
SOURCE_DATE=2024-01-10
START_INCLUSIVE=2023-01-01
TODAY=2024-01-01
UPDATE_ID=27
BRANCH=master

# ##################################################
#  ANSTEAD DECEMBER 2023 ACCRUALS RUN CONFIGURATION
# ##################################################

help:
	@tput clear ; cat makefile

1-update-parameters: update-adh_bucket_name update-end_exclusive update-end_inclusive update-run_date update-snapshot_arn update-source_date update-start_inclusive update-today update-update_id list-parameters

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/adh_bucket_name --value $(ADH_BUCKET_NAME) --type String --overwrite --data-type text

update-end_exclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_exclusive --value $(END_EXCLUSIVE) --type String --overwrite --data-type text

update-end_inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_inclusive --value $(END_INCLUSIVE) --type String --overwrite --data-type text

update-run_date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(RUN_DATE) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

update-source_date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(SOURCE_DATE) --type String --overwrite --data-type text

update-start_inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/start_inclusive --value $(START_INCLUSIVE) --type String --overwrite --data-type text

update-today:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/today --value $(TODAY) --type String --overwrite --data-type text

update-update_id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

2-restore:
	../../bin/execute-bua-steps $(PROFILE) Dec23Acc1 Restore

3-run-warming:
	../../bin/execute-bua-steps $(PROFILE) Dec23Acc2 EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,WarmIndexes,ScaleDown

4-utility-profiles:
	../../bin/execute-bua-steps $(PROFILE) Dec23Acc3 UtilityProfiles,Segments,Snapshot

4-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

5-restore-bua-scripts:
	../../bin/restore-bua-scripts $(PROFILE) bua-scripts.txt

6-run-accruals:
	../../bin/execute-bua-steps $(PROFILE) Dec23Acc4 Microscalar,Snapshot,BasicReads,Snapshot,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,Snapshot,InvoiceRuns,Snapshot,ILIExceptions,ScaleDown,Snapshot,Prepare,Snapshot,Export,ProfileValidation,DumpErrors,SwitchBastion
