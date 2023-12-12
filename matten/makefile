PROFILE=matten
PREFIX=prd-matten
BUCKET=prd-matten-s3-rw-integration
TOPIC=arn:aws:sns:ap-southeast-2:077642019132:prd-matten-sns-bua-notify-topic
NOTIFY_STEPS=Restore
WEEKLY_RUN_STEPS=EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,ScaleDown,UtilityProfiles,Segments,Snapshot,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ILIExceptions,ScaleDown,Prepare,Export,ProfileValidation,DumpErrors

# ###############################################
#      MATTEN PRODUCTION RUN CONFIGURATION
# ###############################################

1-update-parameters: update-adh_bucket_name update-notify_steps list-parameters

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

update-notify_steps:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/notify_steps --value $(NOTIFY_STEPS) --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt

2-trigger-restore:
		AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 sns publish --topic-arn $(TOPIC) --message 'reuse'

4-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE)

5-weekly-run:
	../bin/execute-bua-steps $(PROFILE) DecRun3 $(WEEKLY_RUN_STEPS)
