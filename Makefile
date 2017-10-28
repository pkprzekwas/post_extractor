VENV='./.venv'
ENTRYPOINT='./src/start.py'

run:
	if [ ! -d $(VENV) ]; then \
		make init; \
	fi;
	python $(ENTRYPOINT)

init:
	virtualenv -p python3 .venv ; \
	pip install -r requirements.txt ; \
	python -m textblob.download_corpora ; \

clean:
	rm -rf .venv
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force {} +
