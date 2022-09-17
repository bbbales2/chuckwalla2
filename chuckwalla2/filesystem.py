from s3fs import S3FileSystem


def get_filesystem() -> S3FileSystem:
    fs = S3FileSystem()
    return fs
