SHELL=/bin/bash
SHELLOPTS=errexit:pipefail

ENVDIR=env
ACTIVATE:=$(ENVDIR)/bin/activate

.PHONY:	clean

count=10


PYTHON_EXECUTABLE=python3

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
    VIRTUALENV_EXECUTABLE=virtualenv -p /usr/bin/python3
endif
ifeq ($(UNAME_S), Darwin)
    VIRTUALENV_EXECUTABLE=pyvenv
endif

CONF="../conf/dev.py"


requirements = requirements.txt requirements-dev.txt
virtualenv: $(ACTIVATE)
$(ACTIVATE): $(requirements)
	test -d $(ENVDIR) || $(VIRTUALENV_EXECUTABLE) $(ENVDIR)
	for f in $?; do \
		. $(ACTIVATE); pip install -U -r $$f; \
	done
	touch $(ACTIVATE)

dev: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="$(CONF)" $(PYTHON_EXECUTABLE) runserver.py

test: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="$(CONF)" py.test --cov colombia tests/

shell: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="$(CONF)" $(PYTHON_EXECUTABLE) manage.py shell

dummy: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="$(CONF)" $(PYTHON_EXECUTABLE) manage.py dummy -n $(count)

import: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="$(CONF)" PYTHONPATH=. $(PYTHON_EXECUTABLE) colombia/import.py

submodule:
	test -d doc/_themes/ || git submodule add git://github.com/kennethreitz/kr-sphinx-themes.git doc/_themes

docs: virtualenv submodule
	git submodule update --init
	. $(ACTIVATE); make -C doc/ html
	open doc/_build/html/index.html

clean:
	rm -rf $(ENVDIR)
	rm -rf doc/_build/
