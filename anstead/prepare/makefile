PROFILE=anstead
PREFIX=tst-anstead
BRANCH=master

#
# ANSTEAD TEST PREPARE STEP
#

help:
	@tput clear ; cat makefile

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value '26' --type String --overwrite --data-type text

2-list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

3-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

4-restore-bua-scripts:
	bash ../../bin/restore-bua-scripts $(PROFILE) ../../config/bua-procedures.txt

5-reset-prepare-tables:
	bash ../../bin/reset-prepare-tables $(PROFILE)

6-prepare-only:
	../../bin/execute-bua-steps $(PROFILE) Prepare Prepare
