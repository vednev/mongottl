# MongoDB Batch Deleter — Lambda / EventBridge

Same TTL-style delete logic as the long-running service (`main.py`), adapted for AWS Lambda triggered by an Amazon EventBridge scheduled rule. Each invocation performs a single batch delete and exits. EventBridge handles the cadence.

---

## Prerequisites

- Python 3.11+ (Lambda runtime)
- An AWS account with permissions to create Lambda functions, EventBridge rules, Secrets Manager secrets, and IAM roles
- Your MongoDB URI stored in AWS Secrets Manager as:
  ```json
  { "uri": "mongodb+srv://user:pass@cluster.mongodb.net" }
  ```

---

## Setup

### 1. Store your MongoDB URI in Secrets Manager

```bash
aws secretsmanager create-secret \
    --name mongo/batch-deleter \
    --secret-string '{"uri": "mongodb+srv://user:pass@cluster.mongodb.net"}'
```

### 2. Build the deployment package

Lambda requires dependencies to be bundled with the code.

```bash
pip install -r requirements.txt -t package/
cp lambda.py package/
cd package && zip -r ../function.zip . && cd ..
```

### 3. Create the IAM role

```bash
aws iam create-role \
    --role-name mongo-batch-deleter-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    }'

aws iam attach-role-policy \
    --role-name mongo-batch-deleter-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
    --role-name mongo-batch-deleter-role \
    --policy-name secrets-read \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "arn:aws:secretsmanager:<region>:<account-id>:secret:mongo/batch-deleter*"
        }]
    }'
```

### 4. Deploy the Lambda function

```bash
aws lambda create-function \
    --function-name mongo-batch-deleter \
    --runtime python3.11 \
    --role arn:aws:iam::<account-id>:role/mongo-batch-deleter-role \
    --handler lambda.handler \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --environment Variables='{
        "MONGO_SECRET_NAME": "mongo/batch-deleter",
        "MONGO_DB": "mydb",
        "MONGO_COLLECTION": "mycoll",
        "MONGO_FIELD": "createdAt",
        "MONGO_TTL_SECONDS": "2592000",
        "MONGO_MAX_BATCH": "1000",
        "MONGO_REPL_LAG_HARD_STOP": "200",
        "MONGO_DIRTY_SCALE_START": "0.10",
        "MONGO_DIRTY_HARD_STOP": "0.20"
    }'
```

`--timeout 900` sets the maximum runtime to 15 minutes. Size this generously — a large delete batch can take several minutes.

### 5. Create the EventBridge scheduled rule

```bash
aws events put-rule \
    --name mongo-batch-deleter-schedule \
    --schedule-expression "rate(1 minute)" \
    --state ENABLED

aws lambda add-permission \
    --function-name mongo-batch-deleter \
    --statement-id eventbridge-invoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn arn:aws:events:<region>:<account-id>:rule/mongo-batch-deleter-schedule

aws events put-targets \
    --rule mongo-batch-deleter-schedule \
    --targets "Id=1,Arn=arn:aws:lambda:<region>:<account-id>:function:mongo-batch-deleter"
```

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MONGO_SECRET_NAME` | yes | — | Secrets Manager secret name containing `{"uri": "..."}` |
| `MONGO_DB` | yes | — | Database name |
| `MONGO_COLLECTION` | yes | — | Collection name |
| `MONGO_FIELD` | yes | — | Date field used for TTL comparison |
| `MONGO_TTL_SECONDS` | yes | — | Retention period in seconds |
| `MONGO_MAX_BATCH` | no | `1000` | Max documents to delete per invocation |
| `MONGO_REPL_LAG_HARD_STOP` | no | `200` | Replication lag in seconds that triggers a hard stop |
| `MONGO_DIRTY_SCALE_START` | no | `0.10` | Dirty cache ratio at which batch scaling begins |
| `MONGO_DIRTY_HARD_STOP` | no | `0.20` | Dirty cache ratio at which batch scaling reaches zero |

---

## Throttling behaviour

On each invocation the handler samples the same cluster health signals as `main.py`:

**Hard stops** (invocation exits immediately, nothing deleted):
- Replication lag > `MONGO_REPL_LAG_HARD_STOP` on any secondary
- Write queue depth > 0

**Gradual scaling**: batch size reduced linearly as dirty cache ratio rises between `MONGO_DIRTY_SCALE_START` and `MONGO_DIRTY_HARD_STOP`.

On sharded clusters, metrics are sampled by connecting directly to each relevant shard primary (`directShardOperations` role required). On replica sets, metrics are sampled from the primary directly.

The invocation return value is logged to CloudWatch:

| `status` | `reason` | Meaning |
|---|---|---|
| `skipped` | `hard_stop` | Cluster health hard stop triggered |
| `skipped` | `nothing_eligible` | No documents older than TTL found |
| `skipped` | `utilisation_too_high` | Dirty cache scaling reduced batch to zero |
| `ok` | — | Batch deleted successfully |

---

## Updating the function

```bash
pip install -r requirements.txt -t package/
cp lambda.py package/
cd package && zip -r ../function.zip . && cd ..
aws lambda update-function-code \
    --function-name mongo-batch-deleter \
    --zip-file fileb://function.zip
```

---

## Notes

- The MongoDB client and URI are cached across warm Lambda invocations — a new connection is only established on a cold start.
- An index on `MONGO_FIELD` is strongly recommended. The batch ID fetch filters and sorts on this field every invocation.
- EventBridge's minimum schedule frequency is 1 minute. If your backlog is large, increase `MONGO_MAX_BATCH` rather than invoking more frequently.
- If MongoDB is inside a VPC, the Lambda function must be deployed into the same VPC with appropriate security group rules. Add `--vpc-config` to the `create-function` command accordingly.
