PROFILE=anstead
ENVIRONMENT=tst
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-21-bua-sql-20231001-20231121-20231121-202130
UPDATE_ID=23
BUCKET=tst-anstead-s3-bua

SOURCE_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/561082505378/tst-anstead-sqs-bua-site-basic-dlqueue
TARGET_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/561082505378/tst-anstead-sqs-bua-site-data-dlqueue

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

3a-restore-ili-exceptions-procedure:
	../bin/restore-procedure $(PROFILE) ili_manual_line_item_exceptions
	../bin/restore-procedure $(PROFILE) manual_ili_exception_approval

4-test-run:
	../bin/execute-bua-steps $(PROFILE) Test ILIExceptions

5-test-run:
	../bin/execute-bua-steps $(PROFILE) Test SwitchBastion

5-prepare-test-redrive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 sqs send-message-batch --queue-url $(SOURCE_QUEUE_URL) --entries file://message-batch.json | tee /tmp/test.log

5-test-redrive-1:
	../venv/bin/python ../bin/redrive-sqs.py --profile $(PROFILE) --source $(SOURCE_QUEUE_URL) --target $(TARGET_QUEUE_URL)

5-test-redrive-2:
	../venv/bin/python ../bin/redrive-sqs.py --profile $(PROFILE) --source $(TARGET_QUEUE_URL) --target $(SOURCE_QUEUE_URL)


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
