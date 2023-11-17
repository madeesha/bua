PROFILE=anstead
ENVIRONMENT=tst
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-19-bua-sql-20231001-20231114-20231115-183027
UPDATE_ID=20
BUCKET=tst-anstead-s3-bua

help:
	@cat makefile

#
# TEST BASIC READ
#

1-update-parameters: update-snapshot_arn update-update_id update-adh_bucket_name list-parameters

2-restore:
	../bin/execute-bua-steps $(PROFILE) Restore

3-restore-bua-create-basic-read:
	../bin/restore-procedure $(PROFILE) bua_create_basic_read

4-test-basic-reads:
	../bin/execute-bua-steps $(PROFILE) TestBasic EmptyQueues,ScaleUpWorkflow,WarmStatistics,ScaleDown,Microscalar,BasicReads

5-dump-errors:
	../bin/execute-bua-steps $(PROFILE) DumpErrors

update-update_id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
