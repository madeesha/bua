PROFILE=anstead
ENVIRONMENT=tst
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-21-bua-sql-20231001-20231121-20231121-202130
UPDATE_ID=23
BUCKET=tst-anstead-s3-bua

help:
	@cat makefile

#
# TEST MICROSCALAR & BASIC READS & ILI EXCEPTIONS & PREPARE CHANGES
#

1-update-parameters: update-snapshot_arn update-update_id update-adh_bucket_name list-parameters

2-restore:
	../bin/execute-bua-steps $(PROFILE) Test EmptyQueues,Restore

#3-restore-bua-scripts:
#	bash ../bin/restore-bua-scripts $(PROFILE)

3-restore-procedures:
	../bin/restore-procedure $(PROFILE) bua_create_basic_read
	../bin/restore-procedure $(PROFILE) bua_create_invoice_scalar
	../bin/restore-procedure $(PROFILE) filter_man_ilie_gas_no_pcf_hv
	../bin/restore-procedure $(PROFILE) bua_prepare_export_data

4-test-run:
	../bin/execute-bua-steps $(PROFILE) Test ScaleUpWorkflow,WarmStatistics,ScaleDown,Microscalar

#4-test-utility-profiles:
#	../bin/execute-bua-steps $(PROFILE) Test UtilityProfiles

#5-test-profile-validation:
#	../bin/execute-bua-steps $(PROFILE) Test ProfileValidation

update-update_id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
