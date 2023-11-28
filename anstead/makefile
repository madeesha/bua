RUN_DATE=2023-11-28
TODAY=2023-10-01
START_INCLUSIVE=2022-10-01
END_INCLUSIVE=2023-09-30
END_EXCLUSIVE=2023-10-01
PROFILE=anstead
TOPIC=arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:prod-data-2023-10-01-snapshot
PREFIX=tst-anstead
BUCKET=tst-anstead-s3-rw-integration
BRANCH=release-uat

help:
	@tput clear ; cat makefile

#
# ANSTEAD WEEKLY RUN
#

1-update-parameters: update-today update-run-date update-source-date update-adh_bucket_name update-snapshot_arn update-notify_steps update-start-inclusive update-end-inclusive update-end-exclusive list-parameters

2-trigger-restore:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 sns publish --topic-arn $(TOPIC) --message 'reuse'

# REMEMBER TO WAIT FOR RESTORE TO FINISH, CHANGE LOCAL CONFIG TO POINT TO THE NEW DB
# REMEMBER TO HAVE TST_ANSTEAD_SQL_UPDATE_ID SET CORRECTLY TO POINT TO THE NEW DB

3-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

4-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE)

5-weekly-run:
	../bin/execute-bua-steps $(PROFILE) Weekly 'EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,ScaleDown,UtilityProfiles,Segments,Snapshot,Microscalar,Snapshot,BasicReads,Snapshot,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,Snapshot,InvoiceRuns,Snapshot,ILIExceptions,ScaleDown,Snapshot,Prepare,Snapshot,Export,ProfileValidation,DumpErrors'

update-today:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/today --value $(TODAY) --type String --overwrite --data-type text

update-run-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(RUN_DATE) --type String --overwrite --data-type text

update-source-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(RUN_DATE) --type String --overwrite --data-type text

update-start-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/start_inclusive --value $(START_INCLUSIVE) --type String --overwrite --data-type text

update-end-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_inclusive --value $(END_INCLUSIVE) --type String --overwrite --data-type text

update-end-exclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_exclusive --value $(END_EXCLUSIVE) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT) --type String --overwrite --data-type text

update-notify_steps:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/notify_steps --value 'Restore' --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
