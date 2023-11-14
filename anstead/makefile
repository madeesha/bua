TODAY=$(shell date +'%Y-%m-%d')
UUID=$(shell uuid)
PROFILE=anstead
TOPIC=arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:prod-data-2023-10-01-snapshot
PREFIX=tst-anstead
BUCKET=tst-anstead-s3-rw-integration

#
# ANSTEAD WEEKLY RUN
# REMEMBER TO REFRESH $(HOME)/git/uss/turkey to the latest master before starting
# REMEMBER TO HAVE TST_ANSTEAD_SQL_UPDATE_ID SET CORRECTLY
#

1-update-parameters: update-run-date update-source-date update-adh_bucket_name update-snapshot_arn update-notify_steps list-parameters

2-trigger-restore:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 sns publish --topic-arn $(TOPIC) --message 'reuse'

3-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE)

4-weekly-run:
	../bin/execute-bua-steps $(PROFILE) Weekly 'EmptyQueues,ScaleUpWorkflow,WarmStatistics,ScaleDown,UtilityProfiles,Segments,Snapshot,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,Snapshot,RestartMeterdata,InvoiceRuns,ScaleDown,Snapshot,Prepare,Snapshot,Export'

update-run-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(TODAY) --type String --overwrite --data-type text

update-source-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(TODAY) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT) --type String --overwrite --data-type text

update-notify_steps:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/notify_steps --value 'Restore' --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
