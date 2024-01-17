PROFILE=anstead
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tst-anstead-27-bua-sql-20240101-20240110-20240111-144242
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=28

help:
	@tput clear ; cat makefile

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

2-update-snapshot_arn:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/snapshot_arn --value $(SNAPSHOT) --type String --overwrite --data-type text

3-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

4-restore:
	../../bin/execute-bua-steps $(PROFILE) TestNEM12 Restore

5-test-nem12:
	../../bin/execute-bua-steps $(PROFILE) TestNEM12 ScaleUpMeterdata,WarmStatistics,WarmIndexes,GenerateNEM12,ScaleDown
