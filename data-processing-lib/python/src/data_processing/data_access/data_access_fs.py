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

from typing import Any

from data_processing.utils import MB, GB, TransformUtils, get_logger
from data_processing.data_access import DataAccess

logger = get_logger(__name__)

class DataAccessFS(DataAccess):
    """
    Base class for data access (interface), tailored for Hierarchical ile Systems
    """
    def __init__(
            self,
            d_sets: list[str],
            checkpoint: bool,
            m_files: int,
            n_samples: int,
            files_to_use: list[str],
            files_to_checkpoint: list[str],
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
        super().__init__(d_sets=d_sets, checkpoint=checkpoint, m_files=m_files, n_samples=n_samples,
                         files_to_use=files_to_use)
        self.files_to_checkpoint = files_to_checkpoint

        
    @classmethod
    def validate(**kwargs) -> bool:
        """
        Run configuration parameter validation and can be enhanced by sub-classes to verify specific/additional configurations
        This method implement the validation for the paramers required for ALL data access methods
        Specific implementation of this can be overriden by its sublcass when dealing with parameters that are specific to the subclass
        :return: False if missing any parameter
        """
        ## We only expect a config block with input and output folder
        valid_config = False
        for k, val in kwargs.items():
            if k == 'config':
                valid_config=True
                if val.get("input_folder", "") == "":
                    valid_config = False
                    logger.error(f"data access factory: Could not find input folder in data access config")
                if val.get("output_folder", "") == "":
                    valid_config = False
                    logger.error(f"data access factory: Could not find output folder in data access config")

        return valid_config


    def get_input_folder(self) -> str:
        """
        Get input folder as a string
        :return: input_folder
        """
        assert self.input_folder, "Input Folder has not been set yet  by Child"
        return self.input_folder

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        assert self.output_folder, "Output Folder has not been set yet by Child"
        return self.output_folder


    def _get_files_to_process_internal(self) -> tuple[list[str], dict[str, float], int]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        and number of operation retries.
        Retries are performed on operation failures and are typically due to the resource overload.
        """
        # Check if we are using data sets
        if self.d_sets is not None:
            # get folders for the input
            folders_to_use, retries = self._get_folders_to_use()
            profile = {"max_file_size": 0.0, "min_file_size": 0.0, "total_file_size": 0.0}
            if len(folders_to_use) > 0:
                # if we have valid folders
                path_list = []
                max_file_size = 0
                min_file_size = MB * GB
                total_file_size = 0
                cm_files = self.m_files
                for folder in folders_to_use:
                    plist, profile, retries1 = self._get_input_files(
                        input_path=folder,
                        output_path=self.get_output_location(folder),
                        cm_files=cm_files,
                        min_file_size=min_file_size,
                        max_file_size=max_file_size,
                    )
                    retries += retries1
                    path_list += plist
                    total_file_size += profile["total_file_size"]
                    if len(path_list) >= cm_files > 0:
                        break
                    max_file_size = profile["max_file_size"] * MB
                    min_file_size = profile["min_file_size"] * MB
                    if cm_files > 0:
                        cm_files -= len(plist)
                profile["total_file_size"] = total_file_size
            else:
                path_list = []
        else:
            # Get input files list
            path_list, profile, retries = self._get_input_files(
                input_path=self.get_input_folder(),
                output_path=self.get_output_folder(),
                cm_files=self.m_files,
            )
        return path_list, profile, retries


    def _get_folders_to_use(self) -> tuple[list[str], int]:
        """
        convert data sets to a list of folders to use
        :return: list of folders and retries
        """
        raise NotImplementedError("Subclasses should implement this!")

    def _get_files_folder(
            self,
            path: str,
            files_to_use: list[str],
            cm_files: int,
            max_file_size: int = 0,
            min_file_size: int = MB * GB
    ) -> tuple[list[dict[str, Any]], dict[str, float], int]:
        """
        Support method to get list input files and their profile
        :param path: input path
        :param files_to_use: file extensions to use
        :param max_file_size: max file size
        :param min_file_size: min file size
        :param cm_files: overwrite for the m_files in the class
        :return: tuple of file list, profile and number of retries
        """
        # Get files list.
        p_list = []
        total_input_file_size = 0
        i = 0
        files, retries = self._list_files_folder(path=path)
        for file in files:
            if i >= cm_files > 0:
                break
            # Only use specified files
            f_name = str(file["name"])
            if files_to_use is not None:
                name_extension = TransformUtils.get_file_extension(f_name)
                if name_extension[1] not in files_to_use:
                    continue
            p_list.append(file)
            size = file["size"]
            total_input_file_size += size
            if min_file_size > size:
                min_file_size = size
            if max_file_size < size:
                max_file_size = size
            i += 1
        return (
            p_list,
            {
                "max_file_size": max_file_size / MB,
                "min_file_size": min_file_size / MB,
                "total_file_size": total_input_file_size / MB,
            },
            retries,
        )

    def _get_input_files(
            self,
            input_path: str,
            output_path: str,
            cm_files: int,
            max_file_size: int = 0,
            min_file_size: int = MB * GB,
    ) -> tuple[list[str], dict[str, float], int]:
        """
        Get list and size of files from input path, that do not exist in the output path
        :param input_path: input path
        :param output_path: output path
        :param cm_files: max files to get
        :return: tuple of file list, profile and number of retries
        """
        if not self.checkpoint:
            file_sizes, profile, retries = self._get_files_folder(
                path=input_path,
                files_to_use=self.files_to_use,
                cm_files=cm_files,
                min_file_size=min_file_size,
                max_file_size=max_file_size,
            )
            files = [fs["name"] for fs in file_sizes]
            return files, profile, retries

        pout_list, _, retries1 = self._get_files_folder(
            path=output_path, files_to_use=self.files_to_checkpoint, cm_files=-1
        )
        output_base_names_ext = [file["name"].replace(self.get_output_folder(), self.get_input_folder())
                                 for file in pout_list]
        # In the case of binary transforms, an extension can be different, so just use the file names.
        # Also remove duplicates
        output_base_names = list(set([TransformUtils.get_file_extension(file)[0] for file in output_base_names_ext]))
        p_list = []
        total_input_file_size = 0
        i = 0
        files, _, retries = self._get_files_folder(
            path=input_path, files_to_use=self.files_to_use, cm_files=-1
        )
        retries += retries1
        for file in files:
            if i >= cm_files > 0:
                break
            f_name = file["name"]
            name_extension = TransformUtils.get_file_extension(f_name)
            if self.files_to_use is not None:
                if name_extension[1] not in self.files_to_use:
                    continue
            if name_extension[0] not in output_base_names:
                p_list.append(f_name)
                size = file["size"]
                total_input_file_size += size
                if min_file_size > size:
                    min_file_size = size
                if max_file_size < size:
                    max_file_size = size
                i += 1
        return (
            p_list,
            {
                "max_file_size": max_file_size / MB,
                "min_file_size": min_file_size / MB,
                "total_file_size": total_input_file_size / MB,
            },
            retries,
        )

    def _list_files_folder(self, path: str) -> tuple[list[dict[str, Any]], int]:
        """
        Get files for a given folder and all sub folders
        :param path: path
        :return: List of files
        """
        raise NotImplementedError("Subclasses should implement this!")



    def get_folder_files(
        self, path: str, extensions: list[str] = None, return_data: bool = True
    ) -> tuple[dict[str, bytes], int]:
        """
        Get a list of byte content of files. The path here is an absolute path and can be anywhere.
        :param path: file path
        :param extensions: a list of file extensions to include. If None, then all files from this and
                           child ones will be returned
        :param return_data: flag specifying whether the actual content of files is returned (True), or just
                            directory is returned (False)
        :return: A dictionary of file names/binary content will be returned
        """
        def _get_file_content(name: str, dt: bool) -> tuple[bytes, int]:
            """
            return file content
            :param name: file name
            :param dt: flag to return data or None
            :return: file content, number of retries
            """
            if dt:
                return self.get_file(name)
            return None, 0

        result = {}
        files, _, retries = self._get_files_folder(
            path=path, files_to_use=extensions, cm_files=-1
        )
        for file in files:
            f_name = str(file["name"])
            b, retries1 = _get_file_content(f_name, return_data)
            retries += retries1
            result[f_name] = b
        return result, retries

