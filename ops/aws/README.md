# AWS EC2 Daily Start/Stop Scheduler

CloudFormation template for automated EC2 instance start/stop scheduling via EventBridge + Lambda.

## Deploy

```bash
aws cloudformation deploy \
  --template-file ops/aws/ec2_scheduler.yaml \
  --stack-name trading-ec2-scheduler \
  --parameter-overrides InstanceId=i-XXXXXXXXXXXXXXXXX \
  --capabilities CAPABILITY_IAM
```

## Verify

List the EventBridge rules:

```bash
aws events list-rules --name-prefix trading-ec2
```

Manually invoke the Lambda to test:

```bash
aws lambda invoke --function-name trading-ec2-start-stop \
  --payload '{"action":"start"}' /tmp/out.json && cat /tmp/out.json
```

Confirm the instance transitions to `running`:

```bash
aws ec2 describe-instances --instance-ids i-XXXXXXXXXXXXXXXXX
```

Repeat with `{"action":"stop"}` and verify the instance transitions to `stopped`.

## Pause (without deleting)

Disable the scheduled rules:

```bash
aws events disable-rule --name trading-ec2-start-0900-ist
aws events disable-rule --name trading-ec2-stop-1600-ist
```

## Re-enable

Re-enable the rules:

```bash
aws events enable-rule --name trading-ec2-start-0900-ist
aws events enable-rule --name trading-ec2-stop-1600-ist
```

## Remove entirely

Delete the CloudFormation stack:

```bash
aws cloudformation delete-stack --stack-name trading-ec2-scheduler
```
