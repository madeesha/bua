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

01_restore_database:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 01_restore_database/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

06_set_user_passwords:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 06_set_user_passwords/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

07_stats_sample_pages:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 07_stats_sample_pages/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

08_scale_down_deployments:
	#kubectl -n core scale --replicas=0 deployment --all
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 08_scale_down_deployments/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

09_switch_dns_cli:
	aws --profile anstead route53 change-resource-record-sets --hosted-zone-id Z06477101FOH3N8B2WK6N \
		--change-batch file://route53.json

09_switch_dns_test:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/09_switch_dns.yml s3://tst-anstead-s3-bua/schedule/next/09_switch_dns.yaml

10_scale_up_workflow:
	kubectl -n core scale --replicas=3 deployment workflow -v=6

11_analyse_statistics:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 11_analyse_statistics/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

12_database_warming:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 12_database_warming/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

13_utility_profiles:
	@aws --profile anstead --region ap-southeast-2 s3 cp yaml/trigger_utility_profiles.yml s3://tst-anstead-s3-bua/schedule/next/trigger_utility_profiles.yml

17_scale_up_meterdata:
	kubectl -n core scale --replicas=6 deployment meterdata

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

25-anstead-bua-initiate:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 25_analyse_statistics/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

27-anstead-bua-initiate:
	@cat yaml/restore_database.yml | sed 's/this:.*/this: 27_initiate_warming/' | venv/bin/python yaml2json.py > json/restore_database.json
	@aws --profile anstead --region ap-southeast-2 s3 cp json/restore_database.json s3://tst-anstead-s3-bua/schedule/next/restore_database.json

scale-up-all:
	kubectl -n core scale --replicas=3 deployment workflow
	kubectl -n core scale --replicas=6 deployment meterdata
