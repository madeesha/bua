PROFILE=matten
PREFIX=prd-matten
BUCKET=prd-matten-s3-rw-integration
NOTIFY_STEPS=Restore,EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,ScaleDown,UtilityProfiles,Segments,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ILIExceptions,ScaleDown,Prepare,Export,ProfileValidation,DumpErrors

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
