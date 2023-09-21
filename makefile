all: requirements test lint

clean:
	rm -fr venv

venv:
	python3.10 -m venv venv

requirements: venv
	venv/bin/pip3 install --upgrade pip -r requirements.txt -r runtime-requirements.txt

json: venv
	cat bua/pipeline/actions/bua_restore.yml | venv/bin/python yaml2json.py > sandpit/bua_restore.json

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

000_restore_database:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_000_restore.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

100_scale_up_nodegroup:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_100_scale_up_nodegroup.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

105_scale_up_workflow:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_105_scale_up_workflow.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

110_analyse_statistics:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_110_analyse_statistics.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

130_utility_profiles:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_130_utility_profiles.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

140_jurisdiction_segments:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_140_segments.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

150_segment_jurisdiction_check:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_150_segment_check.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

160_profile_validation:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_160_validation.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

170_resolve_variances:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_170_resolve_variances.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

180_execute_micro_scalar:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_180_micro_scalar.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

190_scale_up_nodegroup:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_190_scale_up_nodegroup.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

195_scale_up_meterdata:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_195_scale_up_meterdata.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

195_wait_for_scale_up_meterdata:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_195_wait_for_scale_up_meterdata.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

200_generate_NEM12_files:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_200_nem12_files.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

210_reset_basic_reads:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_210_reset_basic_reads.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

220_execute_basic_reads:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_220_basic_reads.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

230_invoice_runs:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_230_invoice_runs.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

250_scale_down_deployments:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_250_scale_down_deployments.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

260_scale_down_nodegroup:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_260_scale_down_nodegroup.yml s3://tst-anstead-s3-bua/schedule/next/trigger.yml

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
