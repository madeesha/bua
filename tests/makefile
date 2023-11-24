PROFILE=anstead
ENVIRONMENT=tst
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:prod-data-2023-10-01-snapshot
UPDATE_ID=23
BUCKET=tst-anstead-s3-bua

help:
	@cat makefile

#
# TEST UTILITY PROFILES
#

1-update-parameters: update-snapshot_arn update-update_id update-adh_bucket_name list-parameters

2-restore:
	../bin/execute-bua-steps $(PROFILE) Test EmptyQueues,Restore,CleanUtilityProfile,CleanInvoiceAttribute,CleanWorkflows

3-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE)

4-test-utility-profiles:
	../bin/execute-bua-steps $(PROFILE) Test UtilityProfiles

5-test-profile-validation:
	../bin/execute-bua-steps $(PROFILE) Test ProfileValidation

update-update_id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

update-adh_bucket_name:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(ENVIRONMENT)-$(PROFILE)/bua/adh_bucket_name --value $(BUCKET) --type String --overwrite --data-type text

list-parameters:
	mkdir -p ../sandpit
	../bin/list-bua-parameters $(PROFILE) | tee ../sandpit/$(PROFILE)-parameters.txt
