#
# Public Variables
#

PYTHON_VENV ?= venv
TERRAFORM_STATE_CACHE := $(ROOTDIR)/$(CACHE_DIR)/terraform/$(TERRAFORM_WORKSPACE_NAME)-state.json
COV ?= app 

#
# Functions
#
in_venv = . $(PYTHON_VENV)/bin/activate && $(1)
#
# Integration with root Makefile
#

setup: python_venv
ifneq ($(wildcard ./setup.py),)
test: python_test
test_integration: python_test_integration
endif
distclean: python_distclean

#
# Main Python commands
#

.PHONY: python_test
python_test: $(PYTHON_VENV) install_dependencies requirements-dev.txt
	$(call in_venv,$(PIP) install --no-cache --requirement requirements-dev.txt)
	$(call in_venv,$(PYTEST) --cov=$(COV) \
		-s tests \
		--cov-report xml \
		--cov-report term-missing \
	)

.PHONY: python_venv
python_venv: $(PYTHON_VENV)
	@:

.PHONY: python_distclean
python_distclean:
	rm -rf "$(PYTHON_VENV)"
	rm -rf ".pytest_cache"

#
# Virtual Env
#

install_dependencies: export PIP_PROCESS_DEPENDENCY_LINKS=1
install_dependencies: $(PYTHON_VENV)
	$(call in_venv,$(PIP) install --upgrade 'pip')
ifneq ($(wildcard ./setup.py),)
	$(call in_venv,$(PIP) install --upgrade --upgrade-strategy eager --editable .)
endif

$(PYTHON_VENV):
	$(PYTHON) -m venv "$@"
	touch $(PYTHON_VENV)
