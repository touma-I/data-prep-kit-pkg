# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import random
from typing import Any

import pyarrow as pa
from data_processing.utils import KB, MB, GB, TransformUtils, get_logger

logger = get_logger(__name__)

class DataAccess:
    """
    Base class for data access (interface), defining all the methods
    """
    def __init__(
            self,
            d_sets: list[str],
            checkpoint: bool,
            m_files: int,
            n_samples: int,
            files_to_use: list[str],
    ):
        """
        Create data access class for folder based configuration
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        :param files_to_checkpoint: files extensions of files to use for checkpointing
        """
        self.d_sets = d_sets
        self.checkpoint = checkpoint
        self.m_files = m_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use


    def get_random_file_set(self, n_samples: int, files: list[str]) -> list[str]:
        """
        Get random set of files
        :param n_samples: set size
        :param files: list of original files
        :return: set of randomly selected files
        """
        # Pick files to include
        if len(files) > n_samples:
            # Pick files at random
            files_set = [int(random.random() * len(files)) for _ in range(n_samples)]
        else:
            # use all existing files
            files_set = range(len(files))
        result = [""] * len(files_set)
        index = 0
        for f in files_set:
            result[index] = files[f]
            index += 1
        logger.info(f"Using files {result} to sample data")
        return result

    def get_files_to_process(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        and the number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        if self.get_output_folder() is None:
            logger.warning("Input/Output are not defined, returning empty list")
            return [], {}, 0
        path_list, path_profile, retries = self._get_files_to_process_internal()
        if self.n_samples > 0:
            files = self.get_random_file_set(n_samples=self.n_samples, files=path_list)
            return files, path_profile, retries
        return path_list, path_profile, retries


    def get_table(self, path: str) -> tuple[pa.table, int]:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed and number of operation retries.
                 Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_file(self, path: str) -> tuple[bytes, int]:
        """
        Get file as a byte array
        :param path: file path
        :return: bytes array of file content and number of operation retries
                 Retries are performed on operation failures and are typically due to the resource overload.

        """
        raise NotImplementedError("Subclasses should implement this!")


    def save_file(self, path: str, data: bytes) -> tuple[dict[str, Any], int]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def get_output_location(self, path: str) -> str:
        """
        Get output location based on input
        :param path: input file location
        :return: output file location
        """
        if self.get_output_folder() is None:
            logger.error("Get out put location. S3 configuration is not provided, returning None")
            return None
        return path.replace(self.get_input_folder(), self.get_output_folder())

    def save_table(self, path: str, table: pa.Table) -> tuple[int, dict[str, Any], int]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def save_job_metadata(self, metadata: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """
        Save job metadata
        :param metadata: a dictionary, containing the following keys:
            "pipeline",
            "job details",
            "code",
            "job_input_params",
            "execution_stats",
            "job_output_stats"
        two additional elements:
            "source"
            "target"
        are filled bu implementation
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        raise NotImplementedError("Subclasses should implement this!")

    def sample_input_data(self, n_samples: int = 10) -> tuple[dict[str, Any], int]:
        """
        Sample input data set to get average table size, average doc size, number of docs, etc.
        Note that here we are not reading all of the input documents, but rather randomly pick
        their subset. It gives more precise answer as subset grows, but it takes longer
        :param n_samples: number of samples to use - default 10
        :return: a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
            average table size MB,
            average doc size KB,
            estimated number of docs
        and number of operation retries
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        # get files to process
        path_list, path_profile, retries = self._get_files_to_process_internal()
        # Pick files to sample
        files = self.get_random_file_set(n_samples=n_samples, files=path_list)
        # Read table and compute number of docs and sizes
        number_of_docs = []
        table_sizes = []
        n_tables = 0
        for f in files:
            table, r = self.get_table(path=f)
            retries += r
            if table is not None:
                n_tables += 1
                number_of_docs.append(table.num_rows)
                # As a table size is mostly document, we can consider them roughly the same
                table_sizes.append(table.nbytes)
        # compute averages
        if n_tables == 0:
            av_number_docs = 0
            av_table_size = 0
            av_doc_size = 0
        else:
            av_number_docs = sum(number_of_docs) / n_tables
            av_table_size = sum(table_sizes) / n_tables / MB
            if av_number_docs == 0:
                av_doc_size = 0
            else:
                av_doc_size = av_table_size * MB / av_number_docs / KB
        logger.info(
            f"average number of docs {av_number_docs}, average table size {av_table_size} MB, "
            f"average doc size {av_doc_size} kB"
        )

        # compute number of docs
        number_of_docs = av_number_docs * len(path_list)
        logger.info(f"Estimated number of docs {number_of_docs}")
        return (
            path_profile
            | {
                "average table size MB": av_table_size,
                "average doc size KB": av_doc_size,
                "estimated number of docs": number_of_docs,
            },
            retries,
        )
