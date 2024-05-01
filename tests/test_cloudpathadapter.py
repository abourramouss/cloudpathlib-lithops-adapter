import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from botocore.exceptions import ClientError
from cloudpathlib import S3Path
from lithopsadapter.cloudpathadapter import LithopsS3ClientAdapter

# Define a global bucket variable for all tests
BUCKET = "test-bucket"


@pytest.fixture
def setup_adapter():
    mock_client = Mock()
    adapter = LithopsS3ClientAdapter(lithops_client=mock_client)
    return adapter, mock_client


def test_list_directory(setup_adapter):
    """Test listing directory contents."""
    adapter, mock_client = setup_adapter
    mock_paginator = Mock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": f"{BUCKET}/dir/file1.txt"},
                {"Key": f"{BUCKET}/dir/file2.txt"},
            ],
            "CommonPrefixes": [{"Prefix": f"{BUCKET}/dir/subdir/"}],
        }
    ]
    path = S3Path(f"s3://{BUCKET}/dir")
    results = list(adapter._list_dir(path))
    assert len(results) == 3
    assert results[0][0].key == f"{BUCKET}/dir/file1.txt"
    assert results[1][0].key == f"{BUCKET}/dir/file2.txt"
    assert results[2][0].key == f"{BUCKET}/dir/subdir/"


def test_move_file_same_location(setup_adapter):
    adapter, mock_client = setup_adapter
    src = S3Path(f"s3://{BUCKET}/file.txt")
    dst = S3Path(f"s3://{BUCKET}/file.txt")
    mock_client.copy_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_client.head_object.return_value = {
        "Metadata": {},
        "ETag": '"12345"',
        "LastModified": "2022-01-01T12:00:00Z",
        "ContentLength": "123",
        "ContentType": "text/plain",
    }
    result = adapter._move_file(src, dst)
    assert result == dst


def test_move_file_different_location(setup_adapter):
    """Test moving a file to a different location."""
    adapter, mock_client = setup_adapter
    src = S3Path(f"s3://{BUCKET}/file1.txt")
    dst = S3Path(f"s3://{BUCKET}/file2.txt")
    mock_client.copy_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_client.delete_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }
    result = adapter._move_file(src, dst, remove_src=True)
    mock_client.delete_object.assert_called_with(Bucket=BUCKET, Key="file1.txt")
    assert result.key == "file2.txt"


def test_remove_file(setup_adapter):
    """Test removing a file."""
    adapter, mock_client = setup_adapter
    path = S3Path(f"s3://{BUCKET}/file.txt")
    mock_client.delete_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 204}
    }
    adapter._remove(path)
    mock_client.delete_object.assert_called_with(Bucket=BUCKET, Key="file.txt")


def test_remove_directory(setup_adapter):
    adapter, mock_client = setup_adapter
    path = S3Path(f"s3://{BUCKET}/dir/")

    adapter._list_dir = Mock(
        return_value=[(S3Path(f"s3://{BUCKET}/dir/file1.txt"), False)]
    )

    mock_client.head_object.return_value = {"ContentType": "application/x-directory"}

    mock_client.delete_objects.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 204},
        "Deleted": [{"Key": f"{BUCKET}/dir/file1.txt"}],
        "Errors": [],
    }

    adapter._remove(path)


def test_file_not_found_error(setup_adapter):
    """Test handling of file not found error."""
    adapter, mock_client = setup_adapter
    path = S3Path(f"s3://{BUCKET}/nonexistentfile.txt")
    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    mock_paginator = Mock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = iter(
        [{"Contents": [], "CommonPrefixes": []}]
    )
    assert not adapter._exists(path)


def test_download_file_error(setup_adapter):
    """Test handling of download file error."""
    adapter, mock_client = setup_adapter
    path = S3Path(f"s3://{BUCKET}/file.txt")
    local_path = Path("/fake/local/path")
    mock_client.download_file.side_effect = ClientError({"Error": {}}, "download_file")
    result = adapter._download_file(cloud_path=path, local_path=local_path)
    assert result is None


def test_generate_presigned_url(setup_adapter):
    """Test generating a presigned URL."""
    adapter, mock_client = setup_adapter
    path = S3Path(f"s3://{BUCKET}/file.txt")
    expire_seconds = 3600
    mock_client.generate_presigned_url.return_value = "http://example.com/presigned"
    url = adapter._generate_presigned_url(
        cloud_path=path, expire_seconds=expire_seconds
    )
    assert url == "http://example.com/presigned"
    mock_client.generate_presigned_url.assert_called_with(
        "get_object",
        Params={"Bucket": BUCKET, "Key": "file.txt"},
        ExpiresIn=expire_seconds,
    )
