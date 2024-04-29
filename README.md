# Lithops S3 Client Adapter

This repository contains the Lithops S3 Client Adapter, which allows the cloudpathlib library to interface with Lithops storage abstraction for cloud object storage services like Minio.


[!WARNING]This code is experimental. It is possible that to contain bugs or inconsistencies with the expected behavior of both the Lithops framework and cloudpathlib.

## Getting Started

To use the adapter, ensure you have both Lithops and cloudpathlib installed:

```bash
pip install lithops cloudpathlib
```

You can then create an instance of the `LithopsS3ClientAdapter` by providing a Lithops storage client:

```python
from lithops import Storage
from cloudpathlib import CloudPath

storage = Storage()
minio_client = storage.get_client()

adapter_client = LithopsS3ClientAdapter(minio_client)
cloud_path = CloudPath("s3://your-bucket-name/path", client=adapter_client)
```

## Functionality

This adapter aims to bridge functionality between cloudpathlib and Lithops, allowing operations like reading, writing, and listing objects in cloud storage directly via cloudpathlib.
