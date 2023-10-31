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


#
#
# ANSTEAD STEP FUNCTIONS
#
#

anstead-scale-up-workflow:
	bin/execute-bua-steps anstead ScaleUpWorkflow

anstead-scale-down:
	bin/execute-bua-steps anstead ScaleDown

anstead-utility-profiles:
	bin/execute-bua-steps anstead UtilityProfiles

anstead-segments:
	bin/execute-bua-steps anstead Segments


#
# ANSTEAD WEEKLY RUN
#

anstead-trigger-restore:
	AWS_PROFILE=anstead aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic --message 'reuse'

anstead-restore-bua-scripts:
	bash bin/restore-bua-scripts

anstead-baseline-snapshot:
	AWS_PROFILE=anstead AWS_REGION=ap-southeast-2 aws rds create-db-snapshot --db-snapshot-identifier tst-anstead-15-bua-sql-2023-10-31-baseline --db-instance-identifier tst-anstead-15-bua-sql

anstead-weekly-run:
	bin/execute-bua-steps anstead Weekly 'ScaleUpWorkflow,Warming,ScaleDown,UtilityProfiles,Segments,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ScaleDown,Export'

anstead-invoice-runs:
	bin/execute-bua-steps anstead InvoiceRuns


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
#
#

matten-trigger-weekly:
	AWS_PROFILE=matten aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:077642019132:prd-matten-sns-bua-notify-topic --message 'reuse'


#
#
# MATTEN STEP FUNCTIONS
#
#

matten-scale-up-workflow:
	bin/execute-bua-steps matten ScaleUpWorkflow

matten-scale-down:
	bin/execute-bua-steps matten ScaleDown
