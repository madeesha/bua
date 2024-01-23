PROFILE=anstead
SNAPSHOT=arn:aws:rds:ap-southeast-2:760694178318:snapshot:tmp-prd-earl-1-sql-23-15-Jan-21-2024-11-24-Jan-22-2024-shared-key-encrypted
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=29
SOURCE_DATE=2024-01-23
RUN_DATE=2024-01-23

help:
	@tput clear ; cat makefile

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

2-update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT) --type String --overwrite --data-type text

3-update-source_date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(SOURCE_DATE) --type String --overwrite --data-type text

4-update-run_date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(RUN_DATE) --type String --overwrite --data-type text

3-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt
