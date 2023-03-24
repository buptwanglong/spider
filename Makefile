
PWD := $(shell pwd)
export PYTHONPATH=$PYTHONPATH:$(PWD)

cli:
	@echo $(PWD)
	@echo $(script)
	@echo $(args)
	python spider/bin/cli.py