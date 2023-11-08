TODAY=$(shell date +'%Y-%m-%d')
UUID=$(shell uuid)

all: requirements test lint

clean:
	rm -fr venv

venv:
	python3.10 -m venv venv

requirements: venv
	venv/bin/pip3 install --upgrade pip -r requirements.txt

lint: venv
	chmod u+x cf-lint
	source venv/bin/activate && ./cf-lint

test: venv
	venv/bin/pytest tests
	venv/bin/coverage run --branch -m pytest --durations=0 tests
	venv/bin/coverage report -m

check-workflow-config:
	cat $(HOME)/git/ops/cluster/applications/workflow/prd/matten/values.yaml | sed 's/077642019132/1111111111/g' | sed 's/prd-matten/prefix/g' | sort > /tmp/matten.yaml
	cat $(HOME)/git/ops/cluster/applications/workflow/tst/anstead/values.yaml | sed 's/561082505378/1111111111/g' | sed 's/tst-anstead/prefix/g' | sort > /tmp/anstead.yaml
	diff /tmp/anstead.yaml /tmp/matten.yaml
	rm -f /tmp/anstead.yaml /tmp/matten.yaml

check-meterdata-config:
	cat $(HOME)/git/ops/cluster/applications/meterdata/prd/matten/values.yaml | sed 's/077642019132/1111111111/g' | sed 's/prd-matten/prefix/g' | sort > /tmp/matten.yaml
	cat $(HOME)/git/ops/cluster/applications/meterdata/tst/anstead/values.yaml | sed 's/561082505378/1111111111/g' | sed 's/tst-anstead/prefix/g' | sort > /tmp/anstead.yaml
	diff /tmp/anstead.yaml /tmp/matten.yaml
	rm -f /tmp/anstead.yaml /tmp/matten.yaml

#
# ANSTEAD WEEKLY RUN
# REMEMBER TO REFRESH $(HOME)/git/uss/turkey to the latest master before starting
# REMEMBER TO HAVE TST_ANSTEAD_SQL_UPDATE_ID SET CORRECTLY
#

anstead-update-parameters: anstead-update-run-date anstead-update-source-date anstead-update-adh_bucket_name anstead-update-snapshot_arn anstead-list-parameters

anstead-update-run-date:
	AWS_PROFILE=anstead aws --region ap-southeast-2 ssm put-parameter --name '/tst-anstead/bua/run_date' --value $(TODAY) --type String --overwrite --data-type text

anstead-update-source-date:
	AWS_PROFILE=anstead aws --region ap-southeast-2 ssm put-parameter --name '/tst-anstead/bua/source_date' --value $(TODAY) --type String --overwrite --data-type text

anstead-update-adh_bucket_name:
	AWS_PROFILE=anstead aws --region ap-southeast-2 ssm put-parameter --name '/tst-anstead/bua/adh_bucket_name' --value 'tst-anstead-s3-rw-integration' --type String --overwrite --data-type text

anstead-update-snapshot_arn:
	AWS_PROFILE=anstead aws --region ap-southeast-2 ssm put-parameter --name '/tst-anstead/bua/snapshot_arn' --value 'arn:aws:rds:ap-southeast-2:561082505378:snapshot:prod-data-2023-10-01-snapshot' --type String --overwrite --data-type text

anstead-list-parameters:
	mkdir -p sandpit
	bin/list-bua-parameters anstead | tee sandpit/anstead-parameters.txt

anstead-trigger-restore:
	AWS_PROFILE=anstead aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic --message 'reuse'

anstead-restore-bua-scripts:
	bash bin/restore-bua-scripts anstead

anstead-weekly-run:
	bin/execute-bua-steps anstead Weekly 'ScaleUpWorkflow,WarmStatistics,WarmIndexes,ScaleDown,UtilityProfiles,Segments,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ScaleDown,Prepare,Snapshot,Export'



#
#
# MATTEN EKS CONFIGURATION
#
#

matten-get-configmap:
	eks context matten
	mkdir -p eks/matten
	kubectl -n kube-system get configmap/aws-auth -o yaml > eks/matten/aws-auth-orig.yml

matten-apply-configmap:
	eks context matten
	kubectl apply -f eks/matten/aws-auth-new.yml


#
#
# MATTEN WEEKLY RUN
# REMEMBER TO REFRESH $(HOME)/git/uss/turkey to the latest master before starting
# REMEMBER TO HAVE TST_MATTEN_SQL_UPDATE_ID SET CORRECTLY
#
#

matten-update-parameters: matten-update-run-date matten-update-source-date matten-list-parameters

matten-list-parameters:
	mkdir -p sandpit
	bin/list-bua-parameters matten | tee sandpit/matten-parameters.txt

matten-update-run-date:
	AWS_PROFILE=matten aws --region ap-southeast-2 ssm put-parameter --name '/prd-matten/bua/run_date' --value $(TODAY) --type String --overwrite --data-type text

matten-update-source-date:
	AWS_PROFILE=matten aws --region ap-southeast-2 ssm put-parameter --name '/prd-matten/bua/source_date' --value $(TODAY) --type String --overwrite --data-type text

matten-trigger-restore:
	AWS_PROFILE=matten aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:077642019132:prd-matten-sns-bua-notify-topic --message 'reuse'

matten-restore-bua-scripts:
	bash bin/restore-bua-scripts matten

matten-weekly-run:
	bin/execute-bua-steps matten Weekly 'ScaleUpWorkflow,WarmStatistics,WarmIndexes,ScaleDown,UtilityProfiles,Segments,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ScaleDown,Prepare,Snapshot,Export'

matten-fix-run:
	bin/execute-bua-steps matten FixUp 'InvoiceRuns,ScaleDown,Export'

matten-snapshot:
	bin/execute-bua-steps matten Snapshot

matten-scale-up-workflow:
	bin/execute-bua-steps matten ScaleUpWorkflow

matten-scale-down:
	bin/execute-bua-steps matten ScaleDown
