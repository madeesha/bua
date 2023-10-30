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

# Trigger Restore Pipeline

scale-up-workflow:
	bin/execute-bua-step ScaleUpWorkflow

scale-down:
	bin/execute-bua-step ScaleDown

utility-profiles:
	bin/execute-bua-step UtilityProfiles

segments:
	bin/execute-bua-step Segments

matten-trigger-weekly-restore:
	AWS_PROFILE=matten aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:077642019132:prd-matten-sns-bua-notify-topic --message 'reuse'

#
# ANSTEAD WEEKLY RUN
#

anstead-trigger-restore:
	AWS_PROFILE=anstead aws --region ap-southeast-2 sns publish --topic-arn arn:aws:sns:ap-southeast-2:561082505378:tst-anstead-sns-bua-notify-topic --message 'reuse'

anstead-restore-bua-scripts:
	bash bin/restore-bua-scripts

anstead-weekly-run:
	bin/execute-bua-step Weekly 'ScaleUpWorkflow,Warming,ScaleDown,UtilityProfiles,Segments,Microscalar,BasicReads,ScaleUpMeterdata,GenerateNEM12,RestartMeterdata,InvoiceRuns,ScaleDown,Export'
