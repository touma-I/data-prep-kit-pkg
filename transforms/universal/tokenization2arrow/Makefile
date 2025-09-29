REPOROOT=../../..
# Use make help, to see the available rules
include $(REPOROOT)/transforms/.make.cicd.targets

#
# This is intended to be included across the Makefiles provided within
# a given transform's directory tree,  so must use compatible syntax.
#
################################################################################
# This defines the name of the transform and is used to match against
# expected files and is used to define the transform's image name. 
TRANSFORM_NAME=$(shell basename `pwd`)

################################################################################

TRANSFORM_PYTHON_SRC="-m dpk_$(TRANSFORM_NAME).runtime"
TRANSFORM_RAY_SRC="-m dpk_$(TRANSFORM_NAME).ray.runtime"


run-cli-sample-python:
	# TODO: set env variable HF_TOKEN to download tokenizer from HF
	make venv
	source venv/bin/activate && \
	rm -rf output/ds02 && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).runtime \
            --data_local_config "{ 'input_folder' : 'test-data/ds02/input', 'output_folder' : 'output/ds02'}"

run-cli-sample-ray:
	# TODO: set env variable HF_TOKEN to download tokenizer from HF
	make venv
	source venv/bin/activate && \
	rm -rf output/ds02 && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).ray.runtime \
            --data_local_config "{ 'input_folder' : 'test-data/ds01/input', 'output_folder' : 'output/ds01'}" \
			--run_locally True

run-list-sample:
	# TODO: set env variable HF_TOKEN to download tokenizer from HF
	make venv
	source venv/bin/activate && \
	$(PYTHON) -m dpk_$(TRANSFORM_NAME).runtime \
			--tkn_doc_content_column  curr_file_contents \
			--tkn_doc_id_column pulls_and_issues_id \
            --data_local_config "{ 'input_folder' : 'test-data/list/input', 'output_folder' : 'output'}"

run-image:
	docker run -it --env-file .env -e DPK_LOG_LEVEL=DEBUG tokenization2arrow-ray:latest \
	python -m dpk_tokenization2arrow.ray.runtime \
        --run_locally True \
	--tkn_tokenizer ${TEST_TOKENIZER} \
	--tkn_doc_content_column curr_file_contents \
	--tkn_doc_id_column pulls_and_issues_id \
    	--tkn_chunk_size 20000 \
    	--tkn_tokenizer_args cache_dir=/tmp/tkn \
    	--data_s3_config "{'input_folder': '${TEST_INPUT_FOLDER}', 'output_folder': '${TEST_OUTPUT_FOLDER}'}" \
    	--data_files_to_use "[ '.parquet' ]" \
    	--data_max_files 1 \
    	--data_num_samples -1 \
    	--data_checkpointing False \
    	--runtime_num_workers 5 \
    	--runtime_worker_options "{ 'num_cpus': 0.8, 'max_restarts': 0, 'max_task_retries': 0 }"

