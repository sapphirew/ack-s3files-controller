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

"""Integration tests for the S3 Files FileSystem resource.

Tests cover the full FileSystem lifecycle:
- Create with required fields (bucket, roleARN)
- Wait for available status (ACK.ResourceSynced=True)
- Verify status fields populated (fileSystemID, lifeCycleState, ackResourceMetadata.arn)
- Add policy, verify PutFileSystemPolicy called
- Remove policy, verify DeleteFileSystemPolicy called
- Add synchronization configuration (import/expiration rules)
- Update tags
- Delete FileSystem, verify cleanup

Prerequisites (bootstrapped automatically):
- S3 bucket for the file system
- IAM role with S3 Files trust policy and S3 access
"""

import json
import pytest
import time
import logging

from acktest.resources import random_suffix_name
from acktest.k8s import resource as k8s
from acktest.k8s import condition
from acktest.aws.identity import get_region, get_account_id
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_s3files_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e.bootstrap_resources import get_bootstrap_resources

RESOURCE_PLURAL = "filesystems"

# S3 Files file systems are async — creation can take several minutes
CREATE_WAIT_AFTER_SECONDS = 30
UPDATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10

# Max wait for file system to reach available (up to 10 minutes)
AVAILABLE_WAIT_PERIODS = 30
AVAILABLE_WAIT_PERIOD_LENGTH = 20  # seconds per period

SAMPLE_POLICY_TEMPLATE = '{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{account_id}:root"}},"Action":"s3files:*","Resource":"*"}}]}}'


def _make_sample_policy(account_id):
    """Create a valid S3 Files resource policy for the given account."""
    return SAMPLE_POLICY_TEMPLATE.format(account_id=account_id)


def _get_bucket_arn(bucket_name):
    """Construct the S3 bucket ARN from the bucket name."""
    return f"arn:aws:s3:::{bucket_name}"


def _get_replacements():
    """Build replacement values from bootstrap resources."""
    resources = get_bootstrap_resources()
    replacements = REPLACEMENT_VALUES.copy()
    replacements["BUCKET_ARN"] = _get_bucket_arn(resources.FileSystemBucket.name)
    replacements["ROLE_ARN"] = resources.FileSystemRole.arn
    return replacements


def _wait_for_file_system_available(ref, wait_periods=AVAILABLE_WAIT_PERIODS):
    """Wait for the FileSystem to reach available status via the Synced condition."""
    return k8s.wait_on_condition(
        ref,
        condition.CONDITION_TYPE_RESOURCE_SYNCED,
        "True",
        wait_periods=wait_periods,
        period_length=AVAILABLE_WAIT_PERIOD_LENGTH,
    )


def _get_file_system_status_field(ref, field):
    """Get a field from the FileSystem CR status."""
    cr = k8s.get_resource(ref)
    if cr is None:
        return None
    return cr.get("status", {}).get(field)


def _get_file_system_id(ref):
    """Get the fileSystemID from the CR status."""
    return _get_file_system_status_field(ref, "fileSystemID")


def _get_aws_file_system(s3files_client, file_system_id):
    """Get the file system from AWS using the S3 Files API."""
    try:
        return s3files_client.get_file_system(fileSystemId=file_system_id)
    except s3files_client.exceptions.ResourceNotFoundException:
        return None


def _get_aws_file_system_policy(s3files_client, file_system_id):
    """Get the file system policy using the S3 Files GetFileSystemPolicy API."""
    try:
        resp = s3files_client.get_file_system_policy(fileSystemId=file_system_id)
        return resp.get("policy")
    except s3files_client.exceptions.ResourceNotFoundException:
        return None


def _get_aws_sync_config(s3files_client, file_system_id):
    """Get the synchronization configuration using the S3 Files API."""
    try:
        return s3files_client.get_synchronization_configuration(
            fileSystemId=file_system_id
        )
    except s3files_client.exceptions.ResourceNotFoundException:
        return None


@pytest.fixture(scope="module")
def simple_file_system(s3files_client):
    """Create a simple FileSystem for basic lifecycle tests."""
    resource_name = random_suffix_name("ack-s3files", 24)

    replacements = _get_replacements()
    replacements["FILE_SYSTEM_NAME"] = resource_name

    resource_data = load_s3files_resource(
        "file_system",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    # Teardown
    try:
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted
    except Exception:
        pass


@pytest.fixture(scope="module")
def file_system_with_tags(s3files_client):
    """Create a FileSystem with additional tags for tag management tests."""
    resource_name = random_suffix_name("ack-s3files-tags", 24)

    replacements = _get_replacements()
    replacements["FILE_SYSTEM_NAME"] = resource_name

    resource_data = load_s3files_resource(
        "file_system_with_tags",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    # Teardown
    try:
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted
    except Exception:
        pass


@service_marker
@pytest.mark.canary
class TestFileSystem:
    """E2E tests for the S3 Files FileSystem resource lifecycle."""

    def test_create_and_wait_for_available(self, s3files_client, simple_file_system):
        """Test that creating a FileSystem CR invokes CreateFileSystem and reaches available."""
        (ref, cr) = simple_file_system

        # Wait for the file system to become available
        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach available status (ACK.ResourceSynced=True)"
        condition.assert_synced(ref)

        # Verify status fields are populated
        cr = k8s.get_resource(ref)
        assert cr is not None

        status = cr.get("status", {})
        assert status.get("fileSystemID") is not None, "fileSystemID not populated"
        assert status.get("lifeCycleState") == "available", \
            f"Expected available, got {status.get('lifeCycleState')}"
        assert status.get("creationTime") is not None, "creationTime not populated"
        assert status.get("ownerID") is not None, "ownerID not populated"

        # Verify ACK resource metadata has the ARN
        ack_metadata = status.get("ackResourceMetadata", {})
        assert ack_metadata.get("arn") is not None, "ARN not populated in ackResourceMetadata"

        # Verify the file system exists in AWS
        file_system_id = status["fileSystemID"]
        aws_fs = _get_aws_file_system(s3files_client, file_system_id)
        assert aws_fs is not None, "FileSystem not found in AWS"

    def test_status_fields_populated(self, s3files_client, simple_file_system):
        """Test that all read-only status fields are populated after sync."""
        (ref, cr) = simple_file_system

        assert _wait_for_file_system_available(ref)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        status = cr.get("status", {})

        # Verify all status fields from GetFileSystem are populated
        assert status.get("fileSystemID") is not None, "fileSystemID missing"
        assert status.get("lifeCycleState") is not None, "lifeCycleState missing"
        assert status.get("creationTime") is not None, "creationTime missing"
        assert status.get("ownerID") is not None, "ownerID missing"

        # Verify ACK resource metadata has the ARN
        ack_metadata = status.get("ackResourceMetadata", {})
        assert ack_metadata.get("arn") is not None, "ARN missing from ackResourceMetadata"

        # Cross-check with AWS
        file_system_id = status["fileSystemID"]
        aws_fs = _get_aws_file_system(s3files_client, file_system_id)
        assert aws_fs is not None

    def test_verify_tags_on_create_and_update(self, s3files_client, file_system_with_tags):
        """Test that tags are applied on create and can be updated."""
        (ref, cr) = file_system_with_tags

        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach available status"
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        file_system_arn = cr["status"]["ackResourceMetadata"]["arn"]

        # --- Phase 1: Verify tags on create ---
        resp = s3files_client.list_tags_for_resource(resourceId=file_system_arn)
        aws_tags = {t["key"]: t["value"] for t in resp.get("tags", [])}

        expected_tags = {
            "Environment": "testing",
            "Team": "platform",
            "ManagedBy": "ACK",
        }

        for key, value in expected_tags.items():
            assert key in aws_tags, f"Tag '{key}' not found in AWS tags"
            assert aws_tags[key] == value, \
                f"Tag '{key}' expected '{value}', got '{aws_tags[key]}'"

        # --- Phase 2: Update tags ---
        new_tags = [
            {"key": "Environment", "value": "staging"},
            {"key": "Team", "value": "platform"},
            {"key": "NewTag", "value": "new-value"},
        ]
        updates = {"spec": {"tags": new_tags}}
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach Synced=True after tag update"
        condition.assert_synced(ref)

        # Poll AWS until the tag update propagates
        max_tag_polls = 15
        for _ in range(max_tag_polls):
            resp = s3files_client.list_tags_for_resource(resourceId=file_system_arn)
            aws_tags = {t["key"]: t["value"] for t in resp.get("tags", [])}
            if aws_tags.get("Environment") == "staging":
                break
            time.sleep(AVAILABLE_WAIT_PERIOD_LENGTH)
        else:
            pytest.fail(
                f"Tag update did not propagate after "
                f"{max_tag_polls * AVAILABLE_WAIT_PERIOD_LENGTH}s. "
                f"Current tags: {aws_tags}"
            )

        assert aws_tags.get("Environment") == "staging"
        assert aws_tags.get("NewTag") == "new-value"

    def test_add_policy(self, s3files_client, simple_file_system):
        """Test that adding a policy to the spec invokes PutFileSystemPolicy."""
        (ref, cr) = simple_file_system

        # Ensure file system is available first
        assert _wait_for_file_system_available(ref)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        file_system_id = cr["status"].get("fileSystemID")
        assert file_system_id is not None, "FileSystem ID not available"

        # Get account ID using acktest identity helper
        account_id = get_account_id()
        sample_policy = _make_sample_policy(account_id)

        # Add policy to spec
        updates = {"spec": {"policy": sample_policy}}
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        # Wait for the controller to reconcile and reach Synced=True
        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach Synced=True after policy update"
        condition.assert_synced(ref)

        # Verify the policy in AWS
        aws_policy = _get_aws_file_system_policy(s3files_client, file_system_id)
        assert aws_policy is not None, "Policy not found on file system after sync"

        # Verify the policy content matches (compare as parsed JSON)
        if isinstance(aws_policy, str):
            aws_policy_parsed = json.loads(aws_policy)
        else:
            aws_policy_parsed = aws_policy
        expected_policy_parsed = json.loads(sample_policy)
        assert aws_policy_parsed == expected_policy_parsed, \
            "Policy content does not match"

        # Verify the policy is reflected back in the CR spec
        cr = k8s.get_resource(ref)
        cr_policy = cr.get("spec", {}).get("policy")
        assert cr_policy is not None, "Policy not synced back to CR spec"

    def test_remove_policy(self, s3files_client, simple_file_system):
        """Test that removing the policy from spec invokes DeleteFileSystemPolicy."""
        (ref, cr) = simple_file_system

        assert _wait_for_file_system_available(ref)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        file_system_id = cr["status"].get("fileSystemID")
        assert file_system_id is not None

        # Ensure a policy is attached first (may already be from test_add_policy)
        current_policy = _get_aws_file_system_policy(s3files_client, file_system_id)
        if current_policy is None:
            account_id = get_account_id()
            sample_policy = _make_sample_policy(account_id)
            updates = {"spec": {"policy": sample_policy}}
            k8s.patch_custom_resource(ref, updates)
            time.sleep(UPDATE_WAIT_AFTER_SECONDS)
            assert _wait_for_file_system_available(ref), \
                "FileSystem did not reach Synced=True after adding policy"
            condition.assert_synced(ref)

        # Remove policy by setting to empty string
        updates = {"spec": {"policy": ""}}
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        # Wait for the controller to reconcile the policy removal
        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach Synced=True after policy removal"
        condition.assert_synced(ref)

        # Verify the policy is removed in AWS
        aws_policy = _get_aws_file_system_policy(s3files_client, file_system_id)
        assert aws_policy is None, "Policy still attached after removal"

    def test_add_sync_config(self, s3files_client, simple_file_system):
        """Test that adding importDataRules/expirationDataRules invokes PutSynchronizationConfiguration."""
        (ref, cr) = simple_file_system

        assert _wait_for_file_system_available(ref)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        file_system_id = cr["status"].get("fileSystemID")
        assert file_system_id is not None

        # Add synchronization configuration
        updates = {
            "spec": {
                "importDataRules": [
                    {
                        "prefix": "/imports/",
                        "sizeLessThan": 1073741824,
                        "trigger": "ON_FILE_ACCESS",
                    }
                ],
                "expirationDataRules": [
                    {"daysAfterLastAccess": 30}
                ],
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_AFTER_SECONDS)

        # Wait for the controller to reconcile
        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach Synced=True after sync config update"
        condition.assert_synced(ref)

        # Verify the sync config in AWS
        aws_sync = _get_aws_sync_config(s3files_client, file_system_id)
        assert aws_sync is not None, "SynchronizationConfiguration not found in AWS"

        import_rules = aws_sync.get("importDataRules", [])
        assert len(import_rules) >= 1, "No import data rules found"

        expiration_rules = aws_sync.get("expirationDataRules", [])
        assert len(expiration_rules) >= 1, "No expiration data rules found"

        # Verify latestVersionNumber is populated in CR status
        cr = k8s.get_resource(ref)
        assert cr["status"].get("latestVersionNumber") is not None, \
            "latestVersionNumber not populated after sync config update"

    def test_delete_file_system(self, s3files_client):
        """Test that deleting the CR invokes DeleteFileSystem and cleans up."""
        resource_name = random_suffix_name("ack-s3files-del", 24)

        replacements = _get_replacements()
        replacements["FILE_SYSTEM_NAME"] = resource_name

        resource_data = load_s3files_resource(
            "file_system",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            resource_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)
        assert cr is not None

        # Wait for available
        assert _wait_for_file_system_available(ref), \
            "FileSystem did not reach available before deletion test"
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        file_system_id = cr["status"]["fileSystemID"]

        # Verify file system exists in AWS
        aws_fs = _get_aws_file_system(s3files_client, file_system_id)
        assert aws_fs is not None

        # Delete the K8s resource
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted

        # Wait for AWS deletion to complete
        max_attempts = 30
        wait_seconds = 20

        for _ in range(max_attempts):
            time.sleep(wait_seconds)
            aws_fs = _get_aws_file_system(s3files_client, file_system_id)
            if aws_fs is None:
                return
            lifecycle = aws_fs.get("status")
            if lifecycle == "DELETED":
                return

        pytest.fail(
            f"FileSystem {file_system_id} was not deleted from AWS after "
            f"{max_attempts * wait_seconds} seconds"
        )
