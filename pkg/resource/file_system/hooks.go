// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package file_system

import (
	"context"
	"errors"
	"fmt"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/s3files/types"
	smithy "github.com/aws/smithy-go"

	svcapitypes "github.com/aws-controllers-k8s/s3files-controller/apis/v1alpha1"
	svctags "github.com/aws-controllers-k8s/s3files-controller/pkg/tags"
)

var syncTags = svctags.Tags

// customUpdateFileSystem handles updates for FileSystem resources.
// There is no UpdateFileSystem API — all updates go through sub-resource
// APIs (Policy, SynchronizationConfiguration). Tags are handled automatically
// by the ACK runtime's tag sync mechanism.
func (rm *resourceManager) customUpdateFileSystem(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (*resource, error) {
	// Guard: Do not attempt updates while the FileSystem is in a transitional
	// state. Sub-resource APIs require the FileSystem to be available.
	if latest.ko.Status.LifeCycleState != nil {
		latestState := *latest.ko.Status.LifeCycleState
		if latestState != "available" {
			return nil, ackrequeue.NeededAfter(
				fmt.Errorf("FileSystem is in state '%s', cannot update sub-resources", latestState),
				ackrequeue.DefaultRequeueAfterDuration,
			)
		}
	}

	var err error

	// Handle Policy changes via PutFileSystemPolicy / DeleteFileSystemPolicy
	if delta.DifferentAt("Spec.Policy") {
		desiredPolicy := ""
		if desired.ko.Spec.Policy != nil {
			desiredPolicy = *desired.ko.Spec.Policy
		}
		if desiredPolicy != "" {
			_, err = rm.sdkapi.PutFileSystemPolicy(ctx, &svcsdk.PutFileSystemPolicyInput{
				FileSystemId: latest.ko.Status.FileSystemID,
				Policy:       &desiredPolicy,
			})
			rm.metrics.RecordAPICall("UPDATE", "PutFileSystemPolicy", err)
			if err != nil {
				return nil, err
			}
		} else {
			// Desired policy is empty — remove the existing policy.
			_, err = rm.sdkapi.DeleteFileSystemPolicy(ctx, &svcsdk.DeleteFileSystemPolicyInput{
				FileSystemId: latest.ko.Status.FileSystemID,
			})
			rm.metrics.RecordAPICall("UPDATE", "DeleteFileSystemPolicy", err)
			if err != nil {
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ResourceNotFoundException" {
					// No policy exists — treat as success.
				} else {
					return nil, err
				}
			}
		}
	}

	// Handle SynchronizationConfiguration changes via PutSynchronizationConfiguration
	if delta.DifferentAt("Spec.ImportDataRules") || delta.DifferentAt("Spec.ExpirationDataRules") {
		if desired.ko.Spec.ImportDataRules != nil || desired.ko.Spec.ExpirationDataRules != nil {
			putInput := &svcsdk.PutSynchronizationConfigurationInput{
				FileSystemId: latest.ko.Status.FileSystemID,
			}
			// Pass latestVersionNumber for optimistic concurrency
			if latest.ko.Status.LatestVersionNumber != nil {
				v := int32(*latest.ko.Status.LatestVersionNumber)
				putInput.LatestVersionNumber = &v
			}
			if desired.ko.Spec.ImportDataRules != nil {
				sdkRules := make([]svcsdktypes.ImportDataRule, len(desired.ko.Spec.ImportDataRules))
				for i, r := range desired.ko.Spec.ImportDataRules {
					sdkRule := svcsdktypes.ImportDataRule{}
					if r.Prefix != nil {
						sdkRule.Prefix = r.Prefix
					}
					if r.SizeLessThan != nil {
						sdkRule.SizeLessThan = r.SizeLessThan
					}
					if r.Trigger != nil {
						sdkRule.Trigger = svcsdktypes.ImportTrigger(*r.Trigger)
					}
					sdkRules[i] = sdkRule
				}
				putInput.ImportDataRules = sdkRules
			}
			if desired.ko.Spec.ExpirationDataRules != nil {
				sdkRules := make([]svcsdktypes.ExpirationDataRule, len(desired.ko.Spec.ExpirationDataRules))
				for i, r := range desired.ko.Spec.ExpirationDataRules {
					sdkRule := svcsdktypes.ExpirationDataRule{}
					if r.DaysAfterLastAccess != nil {
						d := int32(*r.DaysAfterLastAccess)
						sdkRule.DaysAfterLastAccess = &d
					}
					sdkRules[i] = sdkRule
				}
				putInput.ExpirationDataRules = sdkRules
			}
			_, err = rm.sdkapi.PutSynchronizationConfiguration(ctx, putInput)
			rm.metrics.RecordAPICall("UPDATE", "PutSynchronizationConfiguration", err)
			if err != nil {
				return nil, err
			}
		}
		// Note: There is no DeleteSynchronizationConfiguration API.
		// If the user removes both fields, the existing config remains
		// on the AWS side. This is a known limitation of the S3 Files API.
	}

	// Handle tag changes via TagResource / UntagResource
	if delta.DifferentAt("Spec.Tags") {
		err = syncTags(
			ctx,
			desired.ko.Spec.Tags, latest.ko.Spec.Tags,
			latest.ko.Status.FileSystemID, convertToOrderedACKTags,
			rm.sdkapi, rm.metrics,
		)
		if err != nil {
			return nil, err
		}
	}

	// No UpdateFileSystem API exists. All updates are handled above via
	// sub-resource APIs and tag sync.
	return desired, nil
}

// Ensure imports are used
var (
	_ = &svcapitypes.FileSystem{}
)
