import json
import os
import pickle
import tarfile
import tempfile
import zipfile
from io import BytesIO as IOReader
from pathlib import Path
from typing import Any, List, Text, Union

from pyneed.exceptions import FileIOException

DEFAULT_ENCODING = 'utf-8'


def write_text_file(
    content: Text,
    file_path: Union[Text, Path],
    encoding: Text = DEFAULT_ENCODING,
    append: bool = False,
) -> None:
    """Writes text to a file.
    Args:
        content: The content to write.
        file_path: The path to which the content should be written.
        encoding: The encoding which should be used.
        append: Whether to append to the file or to truncate the file.
    """
    mode = "a" if append else "w"
    with open(file_path, mode, encoding=encoding) as file:
        file.write(content)


def read_file(filename: Union[Text, Path], encoding: Text = DEFAULT_ENCODING) -> Any:
    """Read text from a file."""

    filepath = Path(filename)
    try:
        with open(filepath, encoding=encoding) as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Failed to read file, " f"'{filepath.resolve()}' does not exist."
        )
    except UnicodeDecodeError:
        raise FileIOException(
            f"Failed to read file '{filepath.resolve()}', "
            f"could not read the file using {encoding} to decode "
            f"it. Please make sure the file is stored with this "
            f"encoding."
        )


def read_json_file(filename: Union[Text, Path]) -> Any:
    """Read json from a file."""
    filepath = Path(filename)
    content = read_file(filepath)
    try:
        return json.loads(content)
    except ValueError as e:
        raise FileIOException(
            f"Failed to read json from '{filepath.resolve()}'. Error: {e}"
        )


def list_directory(path: Union[Text, Path]) -> List[Text]:
    """Returns all files and folders excluding hidden files.
    If the path points to a file, returns the file. This is a recursive
    implementation returning files in any depth of the path."""

    path = Path(path)
    if path.is_file():
        return [path]
    elif path.is_dir():
        results = []
        for base, dirs, files in os.walk(path, followlinks=True):
            # sort files for same order across runs
            files = sorted(files, key=_filename_without_prefix)
            # add not hidden files
            good_files = filter(lambda x: not x.startswith("."), files)
            results.extend(Path(base) / f for f in good_files)
            # add not hidden directories
            good_directories = filter(lambda x: not x.startswith("."), dirs)
            results.extend(Path(base) / f for f in good_directories)
        return results
    else:
        raise ValueError(f"Could not locate the resource '{path.resolve()}'.")


def _filename_without_prefix(file: Text) -> Text:
    """Splits of a filenames prefix until after the first ``_``."""
    return "_".join(file.split("_")[1:])


def pickle_dump(filename: Union[Text, Path], obj: Any) -> None:
    """Saves object to file.
    Args:
        filename: the filename to save the object to
        obj: the object to store
    """
    with open(filename, "wb") as f:
        pickle.dump(obj, f)


def pickle_load(filename: Union[Text, Path]) -> Any:
    """Loads an object from a file.
    Args:
        filename: the filename to load the object from
    Returns: the loaded object
    """
    with open(filename, "rb") as f:
        return pickle.load(f)


def unarchive(byte_array: bytes, directory: Text) -> Text:
    """Tries to unpack a byte array interpreting it as an archive.
    Tries to use tar first to unpack, if that fails, zip will be used."""

    try:
        tar = tarfile.open(fileobj=IOReader(byte_array))
        tar.extractall(directory)
        tar.close()
        return directory
    except tarfile.TarError:
        zip_ref = zipfile.ZipFile(IOReader(byte_array))
        zip_ref.extractall(directory)
        zip_ref.close()
        return directory


def create_temporary_file(data: Any, suffix: Text = "", mode: Text = "w+") -> Text:
    """Creates a tempfile.NamedTemporaryFile object for data.
    mode defines NamedTemporaryFile's  mode parameter in py3."""

    encoding = None if "b" in mode else DEFAULT_ENCODING
    f = tempfile.NamedTemporaryFile(
        mode=mode, suffix=suffix, delete=False, encoding=encoding
    )
    f.write(data)

    f.close()
    return f.name


def create_temporary_directory() -> Text:
    """Creates a tempfile.TemporaryDirectory."""
    f = tempfile.TemporaryDirectory()
    return f.name

