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

# Trigger Restore Pipeline

010_restore_database:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_010_restore.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

130_utility_profiles:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_130_utility_profiles.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

140_jurisdiction_segments:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_140_segments.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

150_segment_jurisdiction_check:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_150_segment_check.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

160_profile_validation:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_160_validation.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

190_execute_micro_scalar:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_190_micro_scalar.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

200_generate_NEM12_files:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_200_nem12_files.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

230_invoice_runs:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_230_invoice_runs.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

# Upgrade Steps

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

22-scale-down:
	kubectl -n core scale --replicas=0 deployment --all

# kubectl edit -n kube-system configmap/aws-auth
#- groups:
#  - system:masters
#  rolearn: arn:aws:iam::561082505378:role/tst_anstead/lambda/tst_anstead_BUA_BUAControllerLambdaExecRole
#  username: Admin

23-dns-switch:
	aws --profile anstead route53 change-resource-record-sets --hosted-zone-id Z06477101FOH3N8B2WK6N \
		--change-batch file://route53.json

24-scale-up-workflow:
	kubectl -n core scale --replicas=3 deployment workflow -v=6

test-bua-initiate-requeue:
	aws --profile anstead lambda invoke \
	--cli-binary-format raw-in-base64-out \
	--function-name tst-anstead-lambda-bua-site-initiate \
	--invocation-type Event \
	--payload '{"run_type": "Requeue", "source_queue": "tst-anstead-sqs-bua-site-data-dlqueue", "target_queue": "tst-anstead-sqs-bua-site-data-queue"}' \
	response.json
	cat response.json
	rm response.json

test-bua-initiate-nem12:
	aws --profile anstead --region ap-southeast-2 lambda invoke \
	--cli-binary-format raw-in-base64-out \
	--function-name tst-anstead-lambda-bua-site-initiate \
	--invocation-type Event \
	--payload '{"run_type": "NEM12", "today": "2023-07-01", "run_date": "2023-08-22", "identifier_type": "SegmentJurisdictionAvgExclEst"}' \
	response.json
	cat response.json
	rm response.json
