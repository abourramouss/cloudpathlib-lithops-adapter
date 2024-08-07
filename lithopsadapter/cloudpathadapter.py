import botocore
import boto3
import os
import logging
import mimetypes
import botocore.session

from botocore.exceptions import ClientError
from pathlib import Path
from cloudpathlib import S3Client, CloudPath, S3Path
from typing import Any, Dict, Union


class CloudPathException(Exception):
    """Base exception for all cloudpathlib custom exceptions."""


class LithopsS3ClientAdapter(S3Client):
    def __init__(
        self,
        lithops_client,
        file_cache_mode=None,
        local_cache_dir=None,
    ):
        self.file_cache_mode = file_cache_mode
        self.local_cache_dir = Path(local_cache_dir or "/tmp")
        self._local_cache_dir = self.local_cache_dir
        logging.basicConfig(level=logging.INFO)
        self.client = lithops_client

    def _get_metadata(self, cloud_path: S3Path) -> Dict[str, Any]:
        try:
            data = self.client.head_object(Bucket=cloud_path.bucket, Key=cloud_path.key)
            return {
                "last_modified": data.get("LastModified"),
                "size": data.get("ContentLength"),
                "etag": data.get("ETag", "").strip('"'),
                "content_type": data.get("ContentType"),
                "extra": data.get("Metadata", {}),
            }
        except ClientError as e:
            logging.error(f"Failed to get metadata for {cloud_path.key}: {e}")
            return {}

    def _download_file(
        self,
        cloud_path: S3Path,
        local_path: Union[str, os.PathLike],
        extra_args={},
        config=None,
    ) -> Path:
        local_path = Path(local_path)
        try:
            kwargs = {"ExtraArgs": extra_args} if extra_args else {}
            kwargs.update({"Config": config} if config else {})
            self.client.download_file(
                Bucket=cloud_path.bucket,
                Key=cloud_path.key,
                Filename=str(local_path),
                **kwargs,
            )
            return local_path
        except ClientError as e:
            logging.error(
                f"Failed to download {cloud_path.key} from {cloud_path.bucket}: {e}"
            )
            return None

    def _is_file_or_dir(self, cloud_path: S3Path) -> str:
        if cloud_path.key.endswith("/"):
            return "dir"
        else:
            try:
                resp = self.client.head_object(
                    Bucket=cloud_path.bucket, Key=cloud_path.key
                )
                if (
                    "ContentType" in resp
                    and resp["ContentType"] == "application/x-directory"
                ):
                    return "dir"
                else:
                    return "file"
            except ClientError as e:
                logging.error(f"Failed to retrieve object metadata: {e}")
                return "unknown"

    def _exists(self, cloud_path: S3Path) -> bool:
        if not cloud_path.key:
            try:
                self.client.head_bucket(Bucket=cloud_path.bucket)
                return True
            except ClientError:
                return False
        else:
            return self._s3_file_query(cloud_path) is not None

    def _s3_file_query(self, cloud_path: S3Path):
        try:
            self.client.head_object(
                Bucket=cloud_path.bucket, Key=cloud_path.key.rstrip("/")
            )
            return "file"
        except ClientError:
            paginator = self.client.get_paginator("list_objects_v2")
            response_iterator = paginator.paginate(
                Bucket=cloud_path.bucket,
                Prefix=cloud_path.key.rstrip("/") + "/",
                Delimiter="/",
                PaginationConfig={"MaxItems": 1},
            )
            for page in response_iterator:
                if page.get("Contents") or page.get("CommonPrefixes"):
                    return "dir"
            return None

    def _list_dir(self, cloud_path, recursive=False):
        bucket = cloud_path.bucket
        prefix = cloud_path.key
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter="/" if not recursive else None
        )

        for page in page_iterator:
            for content in page.get("Contents", []):
                yield CloudPath(f"s3://{bucket}/{content['Key']}"), False
            for common_prefix in page.get("CommonPrefixes", []):
                yield CloudPath(f"s3://{bucket}/{common_prefix['Prefix']}"), True

    def _move_file(self, src: S3Path, dst: S3Path, remove_src: bool = True) -> S3Path:
        if src == dst:
            try:
                metadata = self._get_metadata(src).get("extra", {})
                self.client.copy_object(
                    Bucket=src.bucket,
                    CopySource={"Bucket": src.bucket, "Key": src.key},
                    Key=src.key,
                    Metadata=metadata,
                    MetadataDirective="REPLACE",
                )
            except Exception as e:
                logging.error(f"Failed to update metadata for {src}: {e}")
                return None
        else:
            try:
                self.client.copy_object(
                    Bucket=dst.bucket,
                    CopySource={"Bucket": src.bucket, "Key": src.key},
                    Key=dst.key,
                )
                if remove_src:
                    self.client.delete_object(Bucket=src.bucket, Key=src.key)
            except Exception as e:
                logging.error(f"Failed to move {src} to {dst}: {e}")
                return None

        return dst

    def _remove(self, cloud_path: S3Path, mission_ok: bool = True) -> None:
        logging.info(f"Starting removal of {cloud_path}")
        file_or_dir = self._is_file_or_dir(cloud_path=cloud_path)
        logging.info(f"Determined '{cloud_path}' as a '{file_or_dir}'")

        try:
            if file_or_dir == "file":
                resp = self.client.delete_object(
                    Bucket=cloud_path.bucket, Key=cloud_path.key
                )
                logging.info(f"Delete object response: {resp}")
            elif file_or_dir == "dir":
                objects_to_delete = [
                    {"Key": obj.key}
                    for obj, _ in self._list_dir(cloud_path, recursive=True)
                ]
                logging.info(f"Objects to delete: {objects_to_delete}")
                if objects_to_delete:
                    resp = self.client.delete_objects(
                        Bucket=cloud_path.bucket, Delete={"Objects": objects_to_delete}
                    )
                    logging.info(f"Delete objects response: {resp}")
                else:
                    logging.info("No objects found to delete; exiting")
                    return
            else:
                if not mission_ok:
                    raise CloudPathException(f"Path {cloud_path} does not exist")

            if resp.get("ResponseMetadata", {}).get("HTTPStatusCode") not in (204, 200):
                raise CloudPathException(
                    f"Delete operation failed for {cloud_path} with response: {resp}"
                )
        except Exception as e:
            logging.error(f"Exception during removal: {e}")
            raise

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: S3Path
    ) -> S3Path:
        local_path = Path(local_path)
        file_name = str(local_path)
        content_type, content_encoding = mimetypes.guess_type(file_name)
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if content_encoding:
            extra_args["ContentEncoding"] = content_encoding

        key = cloud_path.key if cloud_path.key else os.path.basename(file_name)

        try:
            self.client.upload_file(
                Filename=file_name,
                Bucket=cloud_path.bucket,
                Key=key,
                ExtraArgs=extra_args,
            )
            return cloud_path
        except botocore.exceptions.ClientError as e:
            logging.error(
                f"Failed to upload {file_name} to {cloud_path.bucket}/{key}: {e}"
            )
            return None

    def _get_public_url(self, cloud_path: S3Path) -> str:
        unsigned_config = botocore.config.Config(signature_version=botocore.UNSIGNED)
        unsigned_client = boto3.client(
            "s3", config=unsigned_config, endpoint_url=cloud_path.endpoint_url
        )
        url = unsigned_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": cloud_path.bucket, "Key": cloud_path.key},
            ExpiresIn=0,
        )
        return url

    def _generate_presigned_url(
        self, cloud_path: S3Path, expire_seconds: int = 3600
    ) -> str:
        url = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": cloud_path.bucket, "Key": cloud_path.key},
            ExpiresIn=expire_seconds,
        )
        return url
