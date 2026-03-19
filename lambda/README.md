# MongoDB Batch Deleter — Lambda / EventBridge

The same `main.py` used for the long-running service also serves as the Lambda entry point. When deployed to AWS Lambda, the runtime calls `handler(event, context)` directly. No separate Lambda file exists.

---

## How it differs from default mode

| Concern | Default mode | Lambda mode |
|---|---|---|
| Config source | CLI arguments | Environment variables |
| MongoDB URI | `--uri` flag | Fetched from Secrets Manager (`MONGO_SECRET_NAME`) |
| MongoClient | Created fresh each run | Cached across warm invocations |
| Scheduling | Caller's responsibility | EventBridge scheduled rule |
| Return value | Logs result and exits | Returns result dict to Lambda runtime |

---

## Prerequisites

- Python 3.11+ Lambda runtime
- An Atlas project Group ID, public key, private key, and cluster name (sharded clusters only)
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

Lambda requires all dependencies to be bundled with the code.

```bash
pip install -r requirements.txt -t package/
cp main.py package/
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

The handler is `main.handler`.

```bash
aws lambda create-function \
    --function-name mongo-batch-deleter \
    --runtime python3.11 \
    --role arn:aws:iam::<account-id>:role/mongo-batch-deleter-role \
    --handler main.handler \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --environment Variables='{
        "MONGO_SECRET_NAME":          "mongo/batch-deleter",
        "MONGO_DB":                   "mydb",
        "MONGO_COLLECTION":           "mycoll",
        "MONGO_FIELD":                "createdAt",
        "MONGO_TTL_SECONDS":          "2592000",
        "MONGO_MAX_BATCH":            "1000",
        "MONGO_REPL_LAG_HARD_STOP":   "200",
        "MONGO_DIRTY_SCALE_START":    "0.10",
        "MONGO_DIRTY_HARD_STOP":      "0.20"
    }'
```

Add Atlas API variables if using a sharded cluster:

```bash
aws lambda update-function-configuration \
    --function-name mongo-batch-deleter \
    --environment Variables='{
        ...,
        "ATLAS_GROUP_ID":      "<24-hex-project-id>",
        "ATLAS_PUBLIC_KEY":    "<public-key>",
        "ATLAS_PRIVATE_KEY":   "<private-key>",
        "ATLAS_CLUSTER_NAME":  "<cluster-name>"
    }'
```

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
| `ATLAS_GROUP_ID` | sharded only | — | Atlas project Group ID |
| `ATLAS_PUBLIC_KEY` | sharded only | — | Atlas API public key |
| `ATLAS_PRIVATE_KEY` | sharded only | — | Atlas API private key |
| `ATLAS_CLUSTER_NAME` | sharded only | — | Atlas cluster name |
| `MONGO_MAX_BATCH` | no | `1000` | Max documents to delete per invocation |
| `MONGO_REPL_LAG_HARD_STOP` | no | `200` | Replication lag in seconds that triggers a hard stop |
| `MONGO_DIRTY_SCALE_START` | no | `0.10` | Dirty cache ratio at which batch scaling begins |
| `MONGO_DIRTY_HARD_STOP` | no | `0.20` | Dirty cache ratio at which batch scaling reaches zero |
| `MONGO_DRY_RUN` | no | `false` | Set to `"true"` to skip actual deletes |

---

## Return values (logged to CloudWatch)

| `status` | `reason` | Meaning |
|---|---|---|
| `skipped` | `hard_stop` | A hard stop signal was active (repl lag, write queue, or disk queue) |
| `skipped` | `utilization_too_high` | Gradual scaling reduced the batch to zero |
| `skipped` | `nothing_eligible` | No documents older than the TTL cutoff were found |
| `dry_run` | — | Dry run — documents counted but not deleted |
| `ok` | — | Batch deleted successfully |

---

## Local testing

Use `--lambda` to invoke the Lambda code path locally without deploying:

```bash
MONGO_SECRET_NAME=mongo/batch-deleter \
MONGO_DB=mydb \
MONGO_COLLECTION=mycoll \
MONGO_FIELD=createdAt \
MONGO_TTL_SECONDS=2592000 \
python main.py --lambda
```

---

## Updating the function

```bash
pip install -r requirements.txt -t package/
cp main.py package/
cd package && zip -r ../function.zip . && cd ..
aws lambda update-function-code \
    --function-name mongo-batch-deleter \
    --zip-file fileb://function.zip
```

---

## Notes

- The MongoClient is cached across warm Lambda invocations. A new connection is established only on a cold start.
- An index on `MONGO_FIELD` is strongly recommended.
- EventBridge's minimum schedule frequency is 1 minute. If your backlog is large, increase `MONGO_MAX_BATCH` rather than invoking more frequently.
- If MongoDB is inside a VPC, deploy the Lambda function into the same VPC with appropriate security group rules. Add `--vpc-config` to the `create-function` command.
