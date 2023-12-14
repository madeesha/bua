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

check-workflow-config-matten:
	cd $(HOME)/git/ops/cluster && git checkout master && git pull
	cat $(HOME)/git/ops/cluster/applications/workflow/prd/matten/values.yaml | sed 's/077642019132/1111111111/g' | sed 's/prd-matten//g' | sed 's/matten//g' | sed 's/82rn3vm3l6-vpce-00c5661b4963f1caa//g' | sort > /tmp/matten.yaml
	cat $(HOME)/git/ops/cluster/applications/workflow/prd/earl/values.yaml | sed 's/760694178318/1111111111/g' | sed 's/prd-earl//g' | sed 's/earl//g' | sed 's/j04rob4qe3-vpce-0c8fca0231c306431//g' | sort > /tmp/earl.yaml
	diff /tmp/earl.yaml /tmp/matten.yaml
	rm -f /tmp/earl.yaml /tmp/matten.yaml

check-meterdata-config-matten:
	cd $(HOME)/git/ops/cluster && git checkout master && git pull
	cat $(HOME)/git/ops/cluster/applications/meterdata/prd/matten/values.yaml | sed 's/077642019132/1111111111/g' | sed 's/prd-matten//g' | sed 's/matten//g' | sed 's/82rn3vm3l6-vpce-00c5661b4963f1caa//g' | sort > /tmp/matten.yaml
	cat $(HOME)/git/ops/cluster/applications/meterdata/prd/earl/values.yaml | sed 's/760694178318/1111111111/g' | sed 's/prd-earl//g' | sed 's/earl//g' | sed 's/j04rob4qe3-vpce-0c8fca0231c306431//g' | sort > /tmp/earl.yaml
	diff /tmp/earl.yaml /tmp/matten.yaml
	rm -f /tmp/earl.yaml /tmp/matten.yaml

check-workflow-config-anstead:
	cd $(HOME)/git/ops/cluster && git checkout master && git pull
	cat $(HOME)/git/ops/cluster/applications/workflow/tst/anstead/values.yaml | sed 's/561082505378//g' | sed 's/tst//g' | sed 's/anstead//g' | sed 's/qdn0xrpa9a//g' | sed 's/9g5wdxqsng//g' | sed 's/05eaeb81b38982bdd//g' | sort > /tmp/anstead.yaml
	cat $(HOME)/git/ops/cluster/applications/workflow/prd/earl/values.yaml | sed 's/760694178318//g' | sed 's/prd//g' | sed 's/earl//g' | sed 's/iwo098k930//g' | sed 's/j04rob4qe3//g' | sed 's/0c8fca0231c306431//g' | sort > /tmp/earl.yaml
	diff /tmp/earl.yaml /tmp/anstead.yaml
	rm -f /tmp/earl.yaml /tmp/anstead.yaml

check-meterdata-config-anstead:
	cd $(HOME)/git/ops/cluster && git checkout master && git pull
	cat $(HOME)/git/ops/cluster/applications/meterdata/tst/anstead/values.yaml | sed 's/561082505378//g' | sed 's/tst//g' | sed 's/anstead//g' | sed 's/82rn3vm3l6//g' | sed 's/zwyumw2q16//g' | sed 's/074a687ae15da74ac//g' | sort > /tmp/anstead.yaml
	cat $(HOME)/git/ops/cluster/applications/meterdata/prd/earl/values.yaml | sed 's/760694178318//g' | sed 's/prd//g' | sed 's/earl//g' | sed 's/j04rob4qe3//g' | sed 's/0c8fca0231c306431//g' | sort > /tmp/earl.yaml
	diff /tmp/earl.yaml /tmp/anstead.yaml
	rm -f /tmp/earl.yaml /tmp/anstead.yaml



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
