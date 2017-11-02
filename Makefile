VENV='./.venv'
ENTRYPOINT='./src/start.py'

run:
	if [ ! -d $(VENV) ]; then \
		make init; \
	fi;
	.venv/bin/python $(ENTRYPOINT)

init:
	virtualenv -p python3 .venv
	.venv/bin/pip install -r requirements.txt
	.venv/bin/python -m textblob.download_corpora

clean:
	rm -rf .venv
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force {} +
