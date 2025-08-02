# SPDX-License-Identifier: Apache-2.0
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

from typing import List, Callable, Any
import json, io, zipfile, os
        
def zipfile_from_ndjson(data: bytearray,
                        keys: List[str] = None,
                        rows: int=-1,
                        prefix: str="") -> bytearray:
    """
    Iterates over an NDJSON file, extracts specified keys from each JSON object,
    and creates an in-memory ZIP file containing the extracted data.

    Args:
        ndjson_filepath (str): The path to the NDJSON file.
        selected_keys (list): A list of keys to extract from each JSON object.

    Returns:
        io.BytesIO: An in-memory BytesIO object containing the ZIP file.
    """
    _zip = io.BytesIO()

    with zipfile.ZipFile(_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        ndx = 0
        for line in data.decode().splitlines():
            if ndx == rows:
                break
            _txt=line.strip()
            if not _txt:
                continue
            try:
               _branch = json.loads(_txt)
               ## Traverse nested keys
               for key in keys:
                   _branch = _branch[key]

               # Create a file-like object for the extracted data
               # Each entry in the zip will be a JSON string of the extracted data
               zf.writestr(f'{prefix}_{ndx}.{keys[-1]}', _branch)
            except json.decoder.JSONDecodeError as e:
               print(f"Error decoding JSON on ndx {ndx}: {e}")
            ndx = ndx + 1

    _zip.seek(0)  # Rewind the BytesIO object to the beginning
    return bytearray(_zip.getvalue())


def rawblocks_from_json(data: bytearray,
                        process_element: Callable[[dict, Any], Any],
                        keys: List[str] = None,
                        rows: int=-1, 
                        **kwargs) -> int:
    """
    Parses out the bytearray as a json structure and retrieves specific elements in the branch(es) specified in the list of keys 
    :param data: byte array to be parsed as an ndjson structure
    :param process_element: Callback function for each identified leaf node
    :param keys: list of keys used as a starting point for identifying branches of interest. if None, then the top level node is used
    :param rows: maximum number of rows processed.
    :param **kwargs: User defined parameters that are passed to the callback function
    :return: set of randomly selected files
    """


    def process_item(key, value):
        """
        Iterate through a dictionary until it gets to the leaf node
        """
        if type(value) is dict:
            for k,v in value.items():
                process_item(f"{key}.{k}", v)
        else:
            process_element({key:value}, **kwargs) 


    ndx=0
    for line in data.decode().splitlines():
        _txt=line.strip()
        if not _txt: 
            continue
        if ndx == rows: 
            break  
        _json=json.loads(_txt)

        ## use root node, if no keys are specified
        if not keys:
            process_item(f"_{ndx}_", _json)

        else: 
            ## Navigate to the specified node. (e.g. ['article_body','html'] _json['article_body']['html']
            _elt = _json
            for nested_key in keys:
                _elt=_elt[nested_key]
            process_item(f"_{ndx}_.{nested_key}", _elt)
        ndx=ndx+1
    return ndx


def write_block(block: dict, file_path) -> Any:
    """
    Wrie the resulting block to disk
    :param block: dictionary with a single key/value pair
    :param file_path: Prefix used for the new file. 
    :return: None
    """
    for k,v in block.items():
        with open(f"{file_path}{k}","w") as fw:
            fw.write(str(v))

def rawtext_from_ndjson(file_path: str, process_element: Callable[[dict, Any], Any], keys: List[str], rows: int=-1):
    """
    Reads an ndjson and parses its content to generate one or more raw text blocks
    :return: result file name
    """
    with open(file_path, 'rb') as f:
        byte_array = f.read()
        rawblocks_from_json(data=byte_array, process_element=process_element, keys=keys, rows=rows, file_path=file_path)


def rawfiles_from_ndjson(file_path: str, keys, rows: int=-1):
     rawtext_from_ndjson(file_path, process_element=write_block, keys=keys, rows=rows)


def htmltext_from_enwiki(file_path: str, process_element: Callable[[dict, Any], Any],keys:List=['article_body','html'], rows: int=-1):
    rawtext_from_ndjson(file_path, keys=keys, rows=rows)


def htmlfiles_from_enwiki(file_path: str, keys:List=['article_body','html'], rows: int=-1):
    """
    for each row, producee the corresponding html payload
    """
    rawfiles_from_ndjson(file_path, keys=keys, rows=rows)


def articlefiles_from_enwiki(file_path: str, rows: int=-1):
    """
    produces one file for the html and one file for the wikitext content
    """
    rawfiles_from_ndjson(file_path, keys=['article_body'], rows=rows)



def test_zipfile_from_enwiki(sourcefile):
    rows = 2
    # this should produce a single zip file with 2 files
    try:
        with open(f'{sourcefile}.zip', 'wb') as fw:
            with open(sourcefile, 'rb') as fs:
                data=bytearray(fs.read())
                assert len(data) > 0
                fw.write(zipfile_from_ndjson(data, keys=['article_body','html'], rows=rows, prefix='test'))
    except FileNotFoundError as e:
        os.remove(f'{sourcefile}.zip')
        raise
    with zipfile.ZipFile(f'{sourcefile}.zip', 'r') as zf:
            assert len(zf.namelist()) == rows



# used for testing
if __name__ == "__main__":
    # this should produce a single file: enwiki_namespace_0_0.ndjson_0_.html
    htmlfiles_from_enwiki("enwiki_namespace_0_0._3.ndjson", keys=['article_body','html'], rows=1)
    
    # this should produce two files: enwiki_namespace_0_0.ndjson_0_article_body.html and enwiki_namespace_0_0.ndjson_0_article_body.wikitext 
    htmlfiles_from_enwiki("enwiki_namespace_0_0._3.ndjson", keys=['article_body'], rows=1)

    # this should produce a single zip file with 2 files
    test_zipfile_from_enwiki("enwiki_namespace_0_0._3.ndjson")
