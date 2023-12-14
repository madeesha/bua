PROFILE=matten
PREFIX=prd-matten
RERUN_PART1=Restore,EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,WarmIndexes,ScaleDown
RERUN_PART2=UtilityProfiles,Segments,Snapshot,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ILIExceptions,ScaleDown,Prepare,ProfileValidation,DumpErrors
UPDATE_ID=16
BRANCH=master

# ###############################################
#      MATTEN PRODUCTION RUN CONFIGURATION
# ###############################################

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

2-rerun-part1:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart1 $(RERUN_PART1)

3-refresh-git:
	cd $(HOME)/git/uss/turkey && git checkout $(BRANCH) && git pull

4-restore-bua-scripts:
	bash ../../bin/restore-bua-scripts $(PROFILE) bua-scripts.txt

5-rerun-part2:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart2 $(RERUN_PART2)

6-rerun-part3:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart3 ScaleUpMeterdata

7-rerun-part4:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart4 ILIExceptions,ScaleDown,Prepare,Export,Snapshot
