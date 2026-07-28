# R48 PLAN — Pull-request CI admission gate

1. Add the missing `pull_request` trigger without changing job topology.
2. Update the job comment to describe push/PR execution.
3. Verify workflow trigger presence with a focused regression test, then run
   lint and the relevant test suite before one atomic commit.
