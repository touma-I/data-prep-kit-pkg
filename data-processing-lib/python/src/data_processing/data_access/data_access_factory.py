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

import argparse
import ast
import uuid
from typing import Union, Any
import importlib

from data_processing.data_access import (
    DataAccess,
)

from data_processing.utils import ParamsUtils, str2bool, get_logger


class DataAccessFactory():
    """
    This class is accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self, cli_arg_prefix: str = "data_", enable_data_navigation: bool = True):
        """
        Create the factory to parse a set of args that will then define the type of DataAccess object
        to be created by the create_data_access() method.
        :param cli_arg_prefix:  if provided, this will be prepended to all the CLI arguments names.
               Make sure it ends with _
        :param enable_data_navigation: if true enables CLI args and configuration for input/output paths,
            data sets, checkpointing, files to use, sampling and max files.
        This allows the creation of transform-specific (or other) DataAccess instances based on the
        transform-specific prefix (e.g. bl_ for blocklist transform).  The resulting keys returned
        in get_input_params() will include the prefix.  The underlying AST or other values of those
        keys is not effected by the prefix.
        """
        #super().__init__(cli_arg_prefix=cli_arg_prefix)
        #self.s3_cred = None
        self.checkpointing = False
        self.dsets = None
        self.max_files = -1
        self.n_samples = -1
        self.files_to_use = []
        self.files_to_checkpoint = []
        self.cli_arg_prefix = cli_arg_prefix
        self.params = {}
        self.logger = get_logger(__name__ + str(uuid.uuid4()))

        self.config = None
        self.enable_data_navigation = enable_data_navigation

    def _validate_config(self, config: dict[str, str]) -> bool:
        """
        Validate that
        :param config: dictionary has at a minimum input and output foder
        :return: True if config is valid, False otherwise
        """
        valid_config = True
        if config.get("input_folder", "") == "":
            valid_config = False
            self.logger.error(f"data access factory {self.cli_arg_prefix}: Could not find input folder in data access config")
        if config.get("output_folder", "") == "":
            valid_config = False
            self.logger.error(f"data access factory {self.cli_arg_prefix}: Could not find output folder in data access config")
        return valid_config

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Define data access specific parameters
        The set of parameters here is a superset of parameters required for all
        supported data access. The user only needs to specify the ones that he needs
        the rest will have the default values
        This might need to be extended if new data access implementation is added
        :param parser: parser
        :return: None
        """

        help_example_dict = {
            "access_key": ["access", "access key help text"],
            "secret_key": ["secret", "secret key help text"],
            "url": ["https://s3.us-east.cloud-object-storage.appdomain.cloud", "optional s3 url"],
            "region": ["us-east-1", "optional s3 region"],
        }

        ## Need to phase out s3_cred. Should be done via Env Variable
        ## Kept her for backward compatibility until we migrate all current transforms
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_cred",
            type=ast.literal_eval,
            default=None,
            help="AST string of options for s3 credentials. Only required for S3 data access.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )

        if self.enable_data_navigation:
            self.__add_data_navigation_params(parser)

    def __add_data_navigation_params(self, parser):
        help_example_dict = {
            "input_folder": [
                "s3-path/your-input-bucket",
                "Path to input folder of files to be processed",
            ],
            "output_folder": [
                "s3-path/your-output-bucket",
                "Path to output folder of processed files",
            ],
        }
        ## Need to phase out s3_config. Kept her for backward compatibility until we migrate all current transforms
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_config",
            type=ast.literal_eval,
            default=None,
            help="AST string containing input/output paths.\n" + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        help_example_dict = {
            "input_folder": ["./input", "Path to input folder of files to be processed"],
            "output_folder": ["/tmp/output", "Path to output folder of processed files"],
        }
        ## Need to phase out local_config. Kept her for backward compatibility until we migrate all current transforms
        parser.add_argument(
            f"--{self.cli_arg_prefix}local_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing input/output folders using local fs.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}data_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing configuration parameters for data access class including input/output folders, etc.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}max_files", type=int, default=-1, help="Max amount of files to process"
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}checkpointing",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="checkpointing flag",
        )
        # In the case of binary files, the resulting extension can be different from the source extension
        # The checkpointing extension is defined here. If multiple files (extensions) are produced from the
        # source files, only the leading one is required here
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_checkpoint",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of file extensions to choose for checkpointing.",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}data_sets",
            type=ast.literal_eval,
            default=None,
            help="List of sub-directories of input directory to use for input. For example, ['dir1', 'dir2']",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_use",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of file extensions to choose for input.",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}num_samples", type=int, default=-1, help="number of random input files to process"
        )

        parser.add_argument(
            f"--{self.cli_arg_prefix}access_class",
            type=str,
            required=False,
            help="ClassName that implements DataAccess API",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}access_module", 
            type=str, 
            required=False,
            help="Module that implements DataAccess Class",
        )


    def apply_input_params(self, args: Union[dict, argparse.Namespace]) -> bool:
        """
        Validate data access specific parameters
        This might need to be extended if new data access implementation is added
        :param args: user defined arguments
        :return: None
        """
        if isinstance(args, argparse.Namespace):
            arg_dict = vars(args)
        elif isinstance(args, dict):
            arg_dict = args
        else:
            raise ValueError("args must be Namespace or dictionary")

        ## We need to phase out local_config and s3_config. For now, keep it for backward compatibility
        s3_config = arg_dict.get(f"{self.cli_arg_prefix}s3_config", None)
        local_config= arg_dict.get(f"{self.cli_arg_prefix}local_config", None)
        self.config = arg_dict.get(f"{self.cli_arg_prefix}data_config", None)
        if self.config is None:
            if s3_config is not None:
                self.config = s3_config
            else:
                self.config = local_config
        if self.config is not None and self.s3_cred is not None:
                self.config = self.config | self.s3_cred

        self.logger.info(f">>>> {arg_dict}")
        self.logger.info(f">>>> data factory {self.cli_arg_prefix}data_config: {self.config}")

        self.checkpointing = arg_dict.get(f"{self.cli_arg_prefix}checkpointing", False)
        self.max_files = arg_dict.get(f"{self.cli_arg_prefix}max_files", -1)
        self.dsets = arg_dict.get(f"{self.cli_arg_prefix}data_sets", None)
        self.n_samples = arg_dict.get(f"{self.cli_arg_prefix}num_samples", -1)
        self.files_to_use = arg_dict.get(f"{self.cli_arg_prefix}files_to_use", [".parquet"])
        self.files_to_checkpoint = arg_dict.get(f"{self.cli_arg_prefix}files_to_checkpoint", [".parquet"])
        self.data_access_class=arg_dict.get(f"{self.cli_arg_prefix}acccess_class", None)
        self.data_access_module=arg_dict.get(f"{self.cli_arg_prefix}acccess_module", None)
        if self.data_access_class is None and self.data_access_module is None:
            self.data_access_module='data_processing.data_access.data_access_local'
            self.data_access_class='DataAccessLocal'

    
        # Check input/output folders are specificed
        if self.config is not None and not self._validate_config(self.config):
            return False
        
        # Check whether both max_files and number samples are defined
        self.logger.info(f"data factory {self.cli_arg_prefix} max_files {self.max_files}, n_sample {self.n_samples}")
        if self.max_files > 0 and self.n_samples > 0:
            self.logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"Both max files {self.max_files} and random samples {self.n_samples} are defined. Only one allowed at a time"
            )
            return False
        if self.dsets is None or len(self.dsets) < 1:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Not using data sets, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}, files to checkpoint {self.files_to_checkpoint}"
            )
        else:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Using data sets {self.dsets}, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}, files to checkpoint {self.files_to_checkpoint}"
            )
        return True
    

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params for metadata
        :return: dictionary of params
        """
        params = {
            "checkpointing": self.checkpointing,
            "max_files": self.max_files,
            "random_samples": self.n_samples,
            "files_to_use": self.files_to_use,
        }
        if self.dsets is not None:
            params["data sets"] = self.dsets
        return params



    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        try:
            if self.data_access_module:
                data_access=getattr(importlib.import_module(self.data_access_module), self.data_access_class)
            else:
                data_access=globals().get(self.data_access_class)
            return data_access(
                config=self.config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
                files_to_checkpoint=self.files_to_checkpoint
            )
        except ImportError:
            self.logger.error(f"Failed to import module {self.data_access_module}")
            raise
        except AttributeError:
            self.logger.error(f"Class {self.data_access_class}  Not found")
            raise
        except Exception:
            self.logger.error(f"Failed to create data access instance {self.data_access_module}.{self.data_access_class}")
            raise

