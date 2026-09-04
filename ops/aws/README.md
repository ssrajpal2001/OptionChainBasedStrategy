# EC2 daily start/stop scheduler (AWS EventBridge + Lambda)

Part of the automated daily lifecycle (see
`docs/superpowers/specs/2026-09-03-fully-automated-daily-lifecycle-design.md`).
Requires the AWS CLI configured with credentials that can deploy CloudFormation
stacks (`cloudformation:*`, `iam:CreateRole`, `lambda:CreateFunction`,
`events:PutRule` at minimum for the initial deploy).

`cron()` expressions in EventBridge run in UTC only — 09:00 IST is 03:30 UTC
same day, 16:00 IST is 10:30 UTC same day. Don't "fix" these to look like IST
times in the template; they're correct as UTC.

## Deploy

    aws cloudformation deploy \
      --template-file ops/aws/ec2_scheduler.yaml \
      --stack-name trading-ec2-scheduler \
      --parameter-overrides InstanceId=i-XXXXXXXXXXXXXXXXX \
      --capabilities CAPABILITY_IAM

Replace `i-XXXXXXXXXXXXXXXXX` with the real instance ID (`aws ec2
describe-instances --query "Reservations[].Instances[].InstanceId"` if you
don't have it handy).

## Verify

    aws events list-rules --name-prefix trading-ec2
    aws lambda invoke --function-name trading-ec2-start-stop \
      --payload '{"action":"start"}' /tmp/out.json && cat /tmp/out.json

Manually invoke with `{"action":"start"}` while the instance is stopped and
confirm via `aws ec2 describe-instances` that it transitions to `running`.
Repeat with `{"action":"stop"}`. Only leave the rules enabled to run on their
real schedule once both are confirmed working.

## Pause (without deleting)

    aws events disable-rule --name trading-ec2-start-0900-ist
    aws events disable-rule --name trading-ec2-stop-1600-ist

## Re-enable

    aws events enable-rule --name trading-ec2-start-0900-ist
    aws events enable-rule --name trading-ec2-stop-1600-ist

## Remove entirely

    aws cloudformation delete-stack --stack-name trading-ec2-scheduler

## Note on market holidays

The schedule is weekday-only (`MON-FRI`), with no market-holiday awareness —
starting the instance on an NSE holiday just means the morning script runs,
finds no market activity, and the day is a no-op past feeder/broker login.
Accepted tradeoff per the original design spec; revisit if holiday noise
becomes annoying.
