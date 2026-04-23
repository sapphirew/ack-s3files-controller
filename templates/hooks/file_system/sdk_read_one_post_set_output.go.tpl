
	// --- Populate LifeCycleState from GetFileSystem response ---
	// The Status field in the API response conflicts with the Kubernetes
	// Status subresource, so it's in ignore.field_paths. We manually map
	// the LifeCycleState here from the resp variable (in scope from sdkFind).
	{
		lifecycleState := string(resp.Status)
		ko.Status.LifeCycleState = &lifecycleState
	}

	// --- Fetch FileSystemPolicy sub-resource ---
	if ko.Status.FileSystemID != nil {
		policyResp, policyErr := rm.sdkapi.GetFileSystemPolicy(ctx, &svcsdk.GetFileSystemPolicyInput{
			FileSystemId: ko.Status.FileSystemID,
		})
		rm.metrics.RecordAPICall("READ_ONE", "GetFileSystemPolicy", policyErr)
		if policyErr != nil {
			var apiErr smithy.APIError
			if errors.As(policyErr, &apiErr) && (apiErr.ErrorCode() == "ResourceNotFoundException" || apiErr.ErrorCode() == "ValidationException") {
				// ResourceNotFoundException: no policy exists.
				// ValidationException: file system is not yet available (e.g. still creating).
				// Both are expected — treat as empty policy.
				ko.Spec.Policy = nil
			} else {
				return nil, policyErr
			}
		} else if policyResp.Policy != nil {
			ko.Spec.Policy = policyResp.Policy
		} else {
			ko.Spec.Policy = nil
		}
	}

	// --- Fetch SynchronizationConfiguration sub-resource ---
	if ko.Status.FileSystemID != nil {
		syncResp, syncErr := rm.sdkapi.GetSynchronizationConfiguration(ctx, &svcsdk.GetSynchronizationConfigurationInput{
			FileSystemId: ko.Status.FileSystemID,
		})
		rm.metrics.RecordAPICall("READ_ONE", "GetSynchronizationConfiguration", syncErr)
		if syncErr != nil {
			var apiErr smithy.APIError
			if errors.As(syncErr, &apiErr) && (apiErr.ErrorCode() == "ResourceNotFoundException" || apiErr.ErrorCode() == "ValidationException") {
				// ResourceNotFoundException: no sync config exists.
				// ValidationException: file system is not yet available (e.g. still creating).
				// Both are expected — treat as empty configuration.
				ko.Spec.ImportDataRules = nil
				ko.Spec.ExpirationDataRules = nil
				ko.Status.LatestVersionNumber = nil
			} else {
				return nil, syncErr
			}
		} else {
			if syncResp.ImportDataRules != nil {
				rules := make([]*svcapitypes.ImportDataRule, len(syncResp.ImportDataRules))
				for i, r := range syncResp.ImportDataRules {
					rule := &svcapitypes.ImportDataRule{}
					if r.Prefix != nil {
						rule.Prefix = r.Prefix
					}
					if r.SizeLessThan != nil {
						rule.SizeLessThan = r.SizeLessThan
					}
					if r.Trigger != "" {
						trigger := string(r.Trigger)
						rule.Trigger = &trigger
					}
					rules[i] = rule
				}
				ko.Spec.ImportDataRules = rules
			} else {
				ko.Spec.ImportDataRules = nil
			}
			if syncResp.ExpirationDataRules != nil {
				rules := make([]*svcapitypes.ExpirationDataRule, len(syncResp.ExpirationDataRules))
				for i, r := range syncResp.ExpirationDataRules {
					rule := &svcapitypes.ExpirationDataRule{}
					if r.DaysAfterLastAccess != nil {
						d := int64(*r.DaysAfterLastAccess)
						rule.DaysAfterLastAccess = &d
					}
					rules[i] = rule
				}
				ko.Spec.ExpirationDataRules = rules
			} else {
				ko.Spec.ExpirationDataRules = nil
			}
			if syncResp.LatestVersionNumber != nil {
				v := int64(*syncResp.LatestVersionNumber)
				ko.Status.LatestVersionNumber = &v
			} else {
				ko.Status.LatestVersionNumber = nil
			}
		}
	}

	// --- Terminal condition on error lifecycle state ---
	if ko.Status.LifeCycleState != nil && *ko.Status.LifeCycleState == "error" {
		msg := "FileSystem is in error state"
		if ko.Status.StatusMessage != nil {
			msg = *ko.Status.StatusMessage
		}
		ackcondition.SetTerminal(&resource{ko}, corev1.ConditionTrue, &msg, nil)
	} else {
		ackcondition.SetTerminal(&resource{ko}, corev1.ConditionFalse, nil, nil)
	}
