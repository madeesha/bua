PROFILE=anstead
SNAPSHOT=arn:aws:rds:ap-southeast-2:760694178318:snapshot:tmp-prd-earl-1-sql-13-00-Jan-31-2024-02-55-Feb-01-2024-shared-key-encrypted
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=32
TODAY=2024-02-01
START_INCLUSIVE=2023-02-01
END_INCLUSIVE=2024-01-31
END_EXCLUSIVE=2024-02-01
SOURCE_DATE=2024-02-01
RUN_DATE=2024-02-01
PARAMS_ID=3

help:
	@tput clear ; cat makefile

1-update-params: 1-update-snapshot-arn 1-update-update-id 1-update-today 1-update-start-inclusive 1-update-end-inclusive 1-update-end-exclusive 1-update-source-date 1-update-run-date 1-update-params-id

1-update-snapshot-arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT) --type String --overwrite --data-type text

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

1-update-today:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/today --value $(TODAY) --type String --overwrite --data-type text

1-update-start-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/start_inclusive --value $(START_INCLUSIVE) --type String --overwrite --data-type text

1-update-end-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_inclusive --value $(END_INCLUSIVE) --type String --overwrite --data-type text

1-update-end-exclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_exclusive --value $(END_EXCLUSIVE) --type String --overwrite --data-type text

1-update-source-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(SOURCE_DATE) --type String --overwrite --data-type text

1-update-run-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(RUN_DATE) --type String --overwrite --data-type text

1-update-params-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/params_id --value $(PARAMS_ID) --type String --overwrite --data-type text

2-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt
