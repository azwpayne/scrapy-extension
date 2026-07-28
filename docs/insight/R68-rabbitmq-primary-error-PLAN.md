# R68 PLAN

Confirm the unpublished-candidate publish-failure handler lets close-time
`BaseException` replace the registration failure, suppress only cleanup errors
on that abort path, add an identity-preserving control-exception regression,
verify, and atomically commit.
