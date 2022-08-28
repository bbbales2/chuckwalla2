import fsspec
from fsspec.implementations.local import LocalFileSystem
from s3fs import S3FileSystem


def get_filesystem(production = False) -> fsspec.AbstractFileSystem:
    if production:
        fs = S3FileSystem()
    else:
        fs = LocalFileSystem()
    return fs
