PROFILE=anstead

help:
	@tput clear ; cat makefile

4-restore-bua-scripts:
	bash ../bin/restore-bua-scripts $(PROFILE) ../config/bua-procedures.txt

5-reset-prepare-tables:
	bash ../bin/reset-prepare-tables $(PROFILE)

list-parameters:
	../bin/list-bua-parameters $(PROFILE) | tee $(PROFILE)-parameters.txt
