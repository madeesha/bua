PROFILE=anstead
SNAPSHOT_ARN=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-35-bua-sql-20240201-20240201-20240208-223905
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=35
SOURCE_DATE=2024-02-01
RUN_DATE=2024-02-01
TODAY=2024-02-01
START_INCLUSIVE=2023-02-01
END_INCLUSIVE=2024-01-31
END_EXCLUSIVE=2024-02-01
PARAMS_ID=3

help:
	@tput clear ; cat makefile

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

1-update-params-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/params_id --value $(PARAMS_ID) --type String --overwrite --data-type text

1-update-snapshot-arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT_ARN) --type String --overwrite --data-type text

1-update-source-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/source_date --value $(SOURCE_DATE) --type String --overwrite --data-type text

1-update-run-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/run_date --value $(RUN_DATE) --type String --overwrite --data-type text

1-update-today-date:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/today --value $(TODAY) --type String --overwrite --data-type text

1-update-start-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/start_inclusive --value $(START_INCLUSIVE) --type String --overwrite --data-type text

1-update-end-inclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_inclusive --value $(END_INCLUSIVE) --type String --overwrite --data-type text

1-update-end-exclusive:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/end_exclusive --value $(END_EXCLUSIVE) --type String --overwrite --data-type text

1-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

2-restore:
	../../bin/execute-bua-steps $(PROFILE) NEM12-Restore Restore,WarmStatistics,WarmIndexes

3-scale-up:
	../../bin/execute-bua-steps $(PROFILE) NEM12-ScaleUp ScaleUpMeterdata

4-test-nem12:
	../../bin/execute-bua-steps $(PROFILE) NEM12-Test GenerateNEM12

5-reset-nem12:
	../../bin/execute-bua-steps $(PROFILE) NEM12-Reset ResetNEM12

6-scale-down:
	../../bin/execute-bua-steps $(PROFILE) NEM12-ScaleDown ScaleDown
