# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

TRANSFORM_NAME=doc_quality

include $(REPOROOT)/transforms/.make.transforms

setup::

clean:: .transforms.clean 

build::

venv:: .defaults.python-lib-src-venv
	source venv/bin/activate && poetry lock && poetry install

image:: 

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=$(DOC_QUALITY_PYTHON_VERSION) TOML_VERSION=$(DOC_QUALITY_PYTHON_VERSION) .transforms.set-versions

publish:: 

test-image:: 

test:: 

test-src:: venv
	source venv/bin/activate && $(PYTEST) test

load-image::

.PHONY: workflow-venv
workflow-venv:
#	$(MAKE) -C kfp_ray workflow-venv

.PHONY: workflow-test
workflow-test:
#	$(MAKE) -C kfp_ray workflow-test

.PHONY: workflow-upload
workflow-upload:
#	$(MAKE) -C kfp_ray workflow-upload

.PHONY: workflow-build
workflow-build:
#	$(MAKE) -C  kfp_ray workflow-build

        	
RUN_ARGS1=--data_local_config "{ 'input_folder' : 'test-data/input', 'output_folder' : 'output'}"
RUN_ARGS2=$(RUN_ARGS1) --docq_text_lang "en"
RUN_ARGS3=$(RUN_ARGS2) --docq_doc_content_column "contents"
RUN_ARGS=$(RUN_ARGS3) --docq_bad_word_filepath "$(TRANSFORM_NAME)/ldnoobw/en"

test-module:: venv
	source venv/bin/activate && python -m $(TRANSFORM_NAME).transform_python $(RUN_ARGS)
	source venv/bin/activate && python -m $(TRANSFORM_NAME).local
	source venv/bin/activate && python -m $(TRANSFORM_NAME).local_python
	
