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

execute_scaleup_workflow:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleUpWorkflow-$(UUID) \
		--input '{"steps": "ScaleUpWorkflow", "run_date": "2023-10-01", "update_id": "14"}'

execute_warming:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Warming-$(UUID) \
		--input '{"steps": "Warming", "run_date": "2023-10-01", "update_id": "14"}'

execute_segments:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Segments-$(UUID) \
		--input '{"steps": "Segments", "run_date": "2023-10-01", "update_id": "14"}'

execute_microscalar:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-MicroScalar-$(UUID) \
		--input '{"steps": "Microscalar", "run_date": "2023-10-01", "update_id": "14"}'

execute_reset_basicreads:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ResetBasicReads-$(UUID) \
		--input '{"steps": "ResetBasicReads", "run_date": "2023-10-01", "update_id": "14"}'

execute_generate_basicreads:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-BasicReads-$(UUID) \
		--input '{"steps": "BasicReads", "run_date": "2023-10-01", "update_id": "14"}'

execute_scaleup_meterdata:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleUpMeterdata-$(UUID) \
		--input '{"steps": "ScaleUpMeterdata", "run_date": "2023-10-01", "update_id": "14"}'

execute_restart_meterdata:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-RestartMeterdata-$(UUID) \
		--input '{"steps": "RestartMeterdata", "run_date": "2023-10-01", "update_id": "14"}'

execute_reset_nem12:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ResetNEM12-$(UUID) \
		--input '{"steps": "ResetNEM12", "run_date": "2023-10-01", "update_id": "14"}'

execute_generate_nem12:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-GenerateNEM12-$(UUID) \
		--input '{"steps": "GenerateNEM12", "run_date": "2023-10-01", "update_id": "14"}'

execute_invoice_runs:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-InvoiceRuns-$(UUID) \
		--input '{"steps": "InvoiceRuns", "run_date": "2023-10-01", "update_id": "14"}'

execute_scaledown:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-ScaleDown-$(UUID) \
		--input '{"steps": "ScaleDown", "run_date": "2023-10-01", "update_id": "14"}'

execute_export:
	@aws --profile anstead --region ap-southeast-2 stepfunctions start-execution \
		--state-machine-arn arn:aws:states:ap-southeast-2:561082505378:stateMachine:tst-anstead-bua \
		--name $(TODAY)-Export-$(UUID) \
		--input '{"steps": "Export", "run_date": "2023-10-01", "update_id": "14"}'

# Manual Upgrade DB Steps

20-restore-bua-scripts:
	bash bin/restore-bua-scripts

21-restore-bua-procedures:
	bash bin/restore-bua-procedures
