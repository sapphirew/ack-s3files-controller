# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
"""Bootstraps the resources required to run the S3Files integration tests.
"""
import logging
import boto3

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.bootstrapping.s3 import Bucket
from acktest.bootstrapping.iam import Role

from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources

# S3 Files is built on EFS technology, so the role must trust the
# elasticfilesystem.amazonaws.com service principal (not s3files).
# The role needs S3 bucket access plus EventBridge permissions for
# change detection between the file system and the S3 bucket.
S3FILES_ROLE_POLICIES = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess",
]


def _enable_bucket_versioning(bucket_name: str):
    """S3 Files requires versioning enabled on the backing bucket."""
    s3 = boto3.client("s3")
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    logging.info(f"Enabled versioning on bucket {bucket_name}")


def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    resources = BootstrapResources(
        FileSystemBucket=Bucket(
            "ack-s3files-e2e-bucket",
        ),
        FileSystemRole=Role(
            "ack-s3files-e2e-role",
            principal_service="elasticfilesystem.amazonaws.com",
            managed_policies=S3FILES_ROLE_POLICIES,
        ),
    )

    try:
        resources.bootstrap()
        _enable_bucket_versioning(resources.FileSystemBucket.name)
    except BootstrapFailureException as ex:
        exit(254)

    return resources

if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
