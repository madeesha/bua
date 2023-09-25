TODAY=$(shell date +'%Y-%m-%d')
UUID=$(shell uuid)

all: requirements test lint

clean:
	rm -fr venv

venv:
	python3.10 -m venv venv

requirements: venv
	venv/bin/pip3 install --upgrade pip -r requirements.txt -r runtime-requirements.txt

lint: venv
	chmod u+x cf-lint
	source venv/bin/activate && ./cf-lint

test: venv
	venv/bin/coverage run --branch -m pytest --durations=0 tests
	venv/bin/coverage report -m

# Trigger Restore Pipeline

execute_restore:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Restore-$(UUID) \
		--input '{"start": "Restore"}'

execute_scaleup_workflow:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleUpWorkflow-$(UUID) \
		--input '{"start": "ScaleUpWorkflow"}'

execute_warming:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Warming-$(UUID) \
		--input '{"start": "Warming"}'

execute_segments:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Segments-$(UUID) \
		--input '{"start": "Segments"}'

execute_microscalar:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-MicroScalar-$(UUID) \
		--input '{"start": "Microscalar"}'

execute_scaleup_meterdata:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleUpMeterdata-$(UUID) \
		--input '{"start": "ScaleUpMeterdata"}'

execute_generate_nem12:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-GenerateNEM12-$(UUID) \
		--input '{"start": "GenerateNEM12"}'

execute_reset_basicreads:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ResetBasicReads-$(UUID) \
		--input '{"start": "ResetBasicReads"}'

execute_generate_basicreads:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-BasicReads-$(UUID) \
		--input '{"start": "BasicReads"}'

execute_invoice_runs:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-InvoiceRuns-$(UUID) \
		--input '{"start": "InvoiceRuns"}'

execute_scaledown:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleDown-$(UUID) \
		--input '{"start": "ScaleDown"}'

# Manual Upgrade DB Steps

18-sync-passwords:
	bash bin/sync-passwords

19-restore-triggers:
	bash bin/restore-triggers

20-restore-bua-scripts:
	bash bin/restore-bua-scripts

21-restore-bua-procedures:
	bash bin/restore-bua-procedures

21b-restore-triggers:
	bash bin/restore-triggers

# kubectl edit -n kube-system configmap/aws-auth
#- groups:
#  - system:masters
#  rolearn: arn:aws:iam::561082505378:role/tst_anstead/lambda/tst_anstead_BUA_BUAControllerLambdaExecRole
#  username: Admin

# Old steps

json: venv
	cat bua/pipeline/actions/bua_restore.yml | venv/bin/python yaml2json.py > sandpit/bua_restore.json

apply-eks: venv
	kubectl apply -f eks/role.yml
	kubectl apply -f eks/rolebinding.yml

edit-config-map: venv
	kubectl edit configmap -n kube-system aws-auth

get-config-map: venv
	kubectl -n kube-system get configmap/aws-auth -o yaml > eks/aws-auth.yml

apply-config-map: venv
	kubectl apply -f eks/aws-auth.yml

watch-nodes: venv
	kubectl get nodes --watch

read-namespaced-deployments: venv
	kubectl describe deployment -n core workflow -v=6

switch-dns-cli:
	aws --profile anstead route53 change-resource-record-sets --hosted-zone-id Z06477101FOH3N8B2WK6N \
		--change-batch file://route53.json
