PROFILE=anstead
SNAPSHOT=arn:aws:rds:ap-southeast-2:561082505378:snapshot:tmp-prd-earl-23-11-30-shared-key-with-basicreads
PREFIX=tst-anstead
BRANCH=master
UPDATE_ID=27

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

5-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

6-restore-bua-scripts:
	../../bin/restore-bua-scripts $(PROFILE) ../../config/bua-procedures.txt

7-test-nem12:
	../../bin/execute-bua-steps $(PROFILE) TestNEM12 ScaleUpMeterdata,WarmStatistics,WarmIndexes,GenerateNEM12,RestartMeterdata,InvoiceRuns,ScaleDown
