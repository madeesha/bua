PROFILE=anstead
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-29-bua-sql-20240101-20240123-20240125-095423
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=30
SOURCE_DATE=2024-01-23
RUN_DATE=2024-01-23
PARAMS_ID=3

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

5-update-params-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/params_id --value $(PARAMS_ID) --type String --overwrite --data-type text

6-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt
