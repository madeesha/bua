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
