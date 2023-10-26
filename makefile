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
	bin/execute-bua-step ScaleUpWorkflow 2023-10-01 14

scale-down:
	bin/execute-bua-step ScaleDown 2023-10-01 14

# Manual Upgrade DB Steps

20-restore-bua-scripts:
	bash bin/restore-bua-scripts

21-restore-bua-procedures:
	bash bin/restore-bua-procedures
