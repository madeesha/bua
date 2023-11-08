TODAY=$(shell date +'%Y-%m-%d')
UUID=$(shell uuid)
PROFILE=anstead
ENVIRONMENT=tst
UPDATE_ID=16
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:prod-data-2023-10-01-snapshot

help:
	@cat makefile

#
# TEST RUN
#

update-parameters: update-run-date update-source-date update-update_id update-adh_bucket_name update-snapshot_arn list-parameters

update-run-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/run_date --value $(TODAY) --type String --overwrite --data-type text

update-source-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/source_date --value $(TODAY) --type String --overwrite --data-type text

update-update_id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/adh_bucket_name --value $(ENVIRONMENT)-$(PROFILE)-s3-bua --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt

restore:
	../bin/execute-bua-steps $(PROFILE) Restore

restore-bua-scripts:
	../bin/restore-bua-scripts $(PROFILE)

scale-up-workflow:
	../bin/execute-bua-steps $(PROFILE) ScaleUpWorkflow

warm-statistics:
	../bin/execute-bua-steps $(PROFILE) WarmStatistics

warm-indexes:
	../bin/execute-bua-steps $(PROFILE) WarmIndexes

scale-down-1:
	../bin/execute-bua-steps $(PROFILE) ScaleDown

utility-profiles:
	../bin/execute-bua-steps $(PROFILE) UtilityProfiles

segments:
	../bin/execute-bua-steps $(PROFILE) Segments

microscalar:
	../bin/execute-bua-steps $(PROFILE) Microscalar

basic-reads:
	../bin/execute-bua-steps $(PROFILE) BasicReads

scale-up-meterdata:
	../bin/execute-bua-steps $(PROFILE) ScaleUpMeterdata

generate-nem12:
	../bin/execute-bua-steps $(PROFILE) GenerateNEM12

restart-meterdata:
	../bin/execute-bua-steps $(PROFILE) RestartMeterdata

invoice-runs:
	../bin/execute-bua-steps $(PROFILE) InvoiceRuns

scale-down-2:
	../bin/execute-bua-steps $(PROFILE) ScaleDown

prepare:
	../bin/execute-bua-steps $(PROFILE) Prepare

snapshot:
	../bin/execute-bua-steps $(PROFILE) Snapshot

export:
	../bin/execute-bua-steps $(PROFILE) Export

destroy:
	../bin/execute-bua-steps $(PROFILE) Destroy
