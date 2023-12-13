PROFILE=matten
PREFIX=prd-matten
RERUN_PART1=Restore,EmptyQueues,CleanWorkflows,CleanInvoiceAttribute,CleanUtilityProfile,ScaleUpWorkflow,WarmStatistics,WarmIndexes,ScaleDown
RERUN_PART2=UtilityProfiles,Segments,Snapshot,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ILIExceptions,ScaleDown,Prepare,ProfileValidation,DumpErrors
UPDATE_ID=16

# ###############################################
#      MATTEN PRODUCTION RUN CONFIGURATION
# ###############################################

1-update-update-id:
	AWS_PROFILE=$(PROFILE) aws --region ap-southeast-2 ssm put-parameter --name /$(PREFIX)/bua/update_id --value $(UPDATE_ID) --type String --overwrite --data-type text

list-parameters:
	../../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt

2-rerun-part1:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart1 $(RERUN_PART1)

3-restore-bua-scripts:
	bash ../../bin/restore-bua-scripts $(PROFILE)

4-rerun-part2:
	../../bin/execute-bua-steps $(PROFILE) ReRunPart2 $(RERUN_PART2)
