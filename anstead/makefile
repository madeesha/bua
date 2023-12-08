RUN_DATE=2023-12-06
TODAY=2023-12-01
START_INCLUSIVE=2022-12-01
END_INCLUSIVE=2023-11-30
END_EXCLUSIVE=2023-12-01
PROFILE=anstead
TOPIC=arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-26-bua-sql-20231201-20231206-20231207-195909
PREFIX=tst-anstead
BUCKET=tst-anstead-s3-bua
BRANCH=release-uat
NOTIFY_STEPS=Restore
WEEKLY_RUN_STEPS=EmptyQueues,ScaleUpWorkflow,WarmStatistics,ScaleDown,Microscalar,Snapshot,BasicReads,Snapshot,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,Snapshot,InvoiceRuns,Snapshot,ILIExceptions,ScaleDown,Snapshot,Prepare,Snapshot,Export,ProfileValidation,DumpErrors,SwitchBastion
BASTION_DNS_ALIAS=tmp-sql.anstead.encore.sh

PREPARE_INPUT_QUEUE=https://sqs.ap-southeast-2.amazonaws.com/561082505378/tst-anstead-sqs-bua-site-prepare-queue
PREPARE_FAILURE_QUEUE=https://sqs.ap-southeast-2.amazonaws.com/561082505378/tst-anstead-sqs-bua-site-prepare-failure-queue

help:
	@tput clear ; cat makefile

#
# ANSTEAD WEEKLY RUN
#

1-update-parameters: update-today update-run-date update-source-date update-adh_bucket_name update-snapshot_arn update-notify_steps update-start-inclusive update-end-inclusive update-end-exclusive update-bastion-dns-alias list-parameters

2-trigger-restore:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 sns publish --topic-arn $(TOPIC) --message 'reuse'

# REMEMBER TO WAIT FOR RESTORE TO FINISH, CHANGE LOCAL CONFIG TO POINT TO THE NEW DB
# REMEMBER TO HAVE TST_ANSTEAD_SQL_UPDATE_ID SET CORRECTLY TO POINT TO THE NEW DB

3-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

4-dump-triggers:
	../bin/dump-triggers matten

4-restore-triggers:
	../bin/restore-triggers anstead matten

4-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE)

5-weekly-run:
	../bin/execute-bua-steps $(PROFILE) Weekly2 $(WEEKLY_RUN_STEPS)

6-clean-up:
	../bin/execute-bua-steps $(PROFILE) CleanUp DumpErrors,SwitchBastion

7-prepare-only:
	../bin/execute-bua-steps $(PROFILE) Prepare Prepare

8-test-nem12-changes:
	../bin/execute-bua-steps $(PROFILE) TestNEM12 ScaleUpWorkflow,WarmStatistics,ScaleUpMeterdata,GenerateNEM12,ScaleDown

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
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/notify_steps --value $(NOTIFY_STEPS) --type String --overwrite --data-type text

update-bastion-dns-alias:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/bastion_dns_alias --value $(BASTION_DNS_ALIAS) --type String --overwrite --data-type text

update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value '26' --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
