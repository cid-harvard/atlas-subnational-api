SHELL=/bin/bash
SHELLOPTS=errexit:pipefail

ENVDIR=env
ACTIVATE:=$(ENVDIR)/bin/activate

.PHONY:	clean

count=10


PYTHON_EXECUTABLE=python3
VIRTUALENV_EXECUTABLE=pyvenv


requirements = requirements.txt requirements-dev.txt
virtualenv: $(ACTIVATE)
$(ACTIVATE): $(requirements)
	test -d $(ENVDIR) || $(VIRTUALENV_EXECUTABLE) $(ENVDIR)
	for f in $?; do \
		. $(ACTIVATE); pip install -r $$f; \
	done
	touch $(ACTIVATE)

dev: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" $(PYTHON_EXECUTABLE) src/runserver.py

test: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" py.test --cov colombia src/tests.py

shell: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" $(PYTHON_EXECUTABLE) src/manage.py shell

dummy: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" $(PYTHON_EXECUTABLE) src/manage.py dummy -n $(count)

submodule:
	test -d doc/_themes/ || git submodule add git://github.com/kennethreitz/kr-sphinx-themes.git doc/_themes

docs: virtualenv submodule
	git submodule update --init
	. $(ACTIVATE); make -C doc/ html
	open doc/_build/html/index.html

clean:
	rm -rf $(ENVDIR)
	rm -rf doc/_build/
