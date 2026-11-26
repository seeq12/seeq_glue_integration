# Seeq ↔ S3 with AWS Glue

Two AWS Glue **Python shell** jobs that move data between Seeq and Amazon S3. Both sign in to Seeq with an
[**Application identity API Key**](https://support.seeq.com/latest/cloud/applications) kept in AWS Secrets Manager, and they share one bucket, one secret and one IAM role.

| Use case                                                  | Direction | What it does                                                                         |
| --------------------------------------------------------- | --------- | ------------------------------------------------------------------------------------ |
| [Data Lab files → S3](#use-case-1-data-lab-files--s3)     | Seeq → S3 | Copies the files in a Data Lab project folder to S3, using the Data Lab contents API |
| [S3 CSV → Seeq signals](#use-case-2-s3-csv--seeq-signals) | S3 → Seeq | Reads a CSV from S3 and pushes each column to Seeq as a signal with SPy              |

Both were tested end to end in November 2026 against a Seeq R66 server, with `seeq-spy` 203.2 on Glue Python shell 3.9.

## Directory structure

```
Seeq_Glue_Integration/
├── README.md                          ← this file
├── iam/                               Shared AWS setup (used by both jobs)
│   ├── trust.json                     Trust policy that lets Glue assume the job role
│   └── policy.json                    Job role permissions: the bucket + the Application identity API Key secret
├── datalab-to-s3/                     Use case 1: Seeq Data Lab → S3
│   └── seeq_datalab_files_to_s3.py    Glue job script (also runs locally)
└── s3-to-seeq/                        Use case 2: S3 CSV → Seeq signals
    ├── s3_csv_to_seeq_signals.py      Glue job script (also runs locally)
    └── sample_signals.csv             Small CSV to test with
```

```
datalab-to-s3:  Data Lab project  files/a.csv           ──►  s3://<bucket>/seeq-datalab/<projectId>/files/a.csv
s3-to-seeq:     s3://<bucket>/seeq-import/reactor.csv   ──►  Seeq signals in datasource "AWS Glue S3 CSV Import"
```

## Prerequisites

- **Seeq**
  - An [Application identity API Key](https://support.seeq.com/latest/cloud/applications), created in Seeq.
  - For use case 1, the Data Lab project must be shared with the Application identity API Key's user. **Read** access
    is enough.
  - The Seeq server must be reachable from the public internet. A Glue job with no VPC connection only has public
    egress. For a private server, add a Glue network connection.
- **AWS**
  - Permission to create IAM roles and to `iam:PassRole` them to Glue. Many organizations restrict this; if so, use a
    sandbox account or ask an AWS admin.
  - AWS CLI v2.

## Setup
Run everything in this README from the repo root in a bash shell (Git Bash on Windows). Set the variables first:

```bash
export AWS_PROFILE=<your-profile> AWS_REGION=<your-region> MSYS_NO_PATHCONV=1   # MSYS_NO_PATHCONV stops Git Bash mangling "/aws-glue/..." paths
ACCT=$(aws sts get-caller-identity --query Account --output text)
SEEQ_URL=https://<your-server>.seeq.site
BUCKET=seeq-glue-$ACCT               # bucket names are global, so the account ID keeps it unique
ROLE=seeq-glue-job-role
SECRET=seeq/glue/access-key
TAGS='Key=Owner,Value=<you> Key=Purpose,Value=seeq-glue'
```

### 1. Create the S3 bucket

```bash
# In us-east-1, drop the --create-bucket-configuration argument
aws s3api create-bucket --bucket $BUCKET --create-bucket-configuration LocationConstraint=$AWS_REGION
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

### 2. Create the secret and store the Application identity API Key

```bash
aws secretsmanager create-secret --name $SECRET --tags $TAGS \
  --description "Seeq Application identity API Key for Seeq <-> S3 Glue jobs. JSON: {access_key, access_key_password}"
```

```bash
aws secretsmanager put-secret-value --secret-id $SECRET \
  --secret-string '{"access_key":"<KEY>","access_key_password":"<PASSWORD>"}'
```

### 3. Create the IAM role for the jobs

`iam/policy.json` limits the role to this bucket (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`) and this one secret,
which covers both jobs. It's a template: `BUCKET_NAME`, `REGION`, `ACCOUNT_ID` and `SECRET_NAME` are filled in from the
variables above when the policy is applied. The secret ARN ends in `-*` because Secrets Manager adds a random suffix to
the name.

```bash
aws iam create-role --role-name $ROLE --assume-role-policy-document file://iam/trust.json --tags $TAGS
aws iam attach-role-policy --role-name $ROLE --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name $ROLE --policy-name seeq-glue --policy-document "$(sed \
  -e "s|BUCKET_NAME|$BUCKET|g" -e "s|REGION|$AWS_REGION|g" -e "s|ACCOUNT_ID|$ACCT|g" -e "s|SECRET_NAME|$SECRET|g" \
  iam/policy.json)"
```

### Waiting for a run and reading its logs

Both use cases start a run the same way. To wait for it and read the logs:

```bash
until s=$(aws glue get-job-run --job-name $JOB --run-id $RUN --query JobRun.JobRunState --output text); \
  [[ "$s" =~ ^(SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED)$ ]]; do sleep 15; done
aws glue get-job-run --job-name $JOB --run-id $RUN --query "JobRun.[JobRunState,ExecutionTime,ErrorMessage]" --output text

# Log stream name = run ID. stdout goes to .../output; tracebacks go to .../error.
aws logs get-log-events --log-group-name /aws-glue/python-jobs/output --log-stream-name $RUN --query "events[].message" --output text
aws logs get-log-events --log-group-name /aws-glue/python-jobs/error  --log-stream-name $RUN --query "events[].message" --output text
```

On Windows, `aws logs` can fail with `'charmap' codec can't encode` when pip's progress bars are in the log. Set
`PYTHONIOENCODING=utf-8`, or read the logs with boto3.

---

## Use case 1: Data Lab files → S3

[`datalab-to-s3/seeq_datalab_files_to_s3.py`](datalab-to-s3/seeq_datalab_files_to_s3.py) lists the files in a folder of
a Seeq Data Lab project (default `files`) and copies each one to S3. It uses only `requests` and `boto3`; SPy isn't
needed.

### How it works

1. Reads the Seeq Application identity API Key from AWS Secrets Manager.
2. Calls `POST {server}/api/auth/login` with `{"username": <API Key>, "password": <API Key password>}`. Content
   type must be `application/vnd.seeq.v1+json`; plain `application/json` returns **415**. The `x-sq-auth` response
   header is the session token.
3. Gets the server and project ID from the Data Lab URL. Any URL inside the project works, e.g.
   `https://<your-server>.seeq.site/data-lab/<PROJECT-ID>/lab`.
4. Lists the folder with the Data Lab (Jupyter) contents API: `GET {server}/data-lab/{projectId}/api/contents/files`. It
   goes into subfolders unless `--RECURSIVE false`.
5. Downloads each file as base64 (`?type=file&format=base64`) and writes it to S3 with `put_object`. The S3 object gets
   `seeq-project-id`, `seeq-path` and `seeq-last-modified` metadata.
6. Logs out with `DELETE /api/auth/logout`. The job fails if any file failed to copy.

If the Data Lab project isn't running, the first request starts it. 502/503/504 responses are retried with backoff (6
attempts in total).

### Job parameters

| Parameter                                          | Required | Default                             | Notes                                                              |
| -------------------------------------------------- | -------- | ----------------------------------- | ------------------------------------------------------------------ |
| `--DATALAB_URL`                                    | yes      |                                     | Any URL inside the Data Lab project                                |
| `--S3_BUCKET`                                      | yes      |                                     | Destination bucket                                                 |
| `--SEEQ_SECRET_ID`                                 | \*       |                                     | Secret JSON: `{"access_key": "...", "access_key_password": "..."}` |
| `--SEEQ_ACCESS_KEY` / `--SEEQ_ACCESS_KEY_PASSWORD` | \*       |                                     | Alternative to the secret. Use for local testing only.             |
| `--SOURCE_FOLDER`                                  | no       | `files`                             | Folder inside the project                                          |
| `--S3_PREFIX`                                      | no       | `seeq-datalab/<projectId>/<folder>` | Key prefix in the bucket                                           |
| `--RECURSIVE`                                      | no       | `true`                              | Go into subfolders                                                 |
| `--VERIFY_SSL`                                     | no       | `true`                              |                                                                    |

\* Provide either `--SEEQ_SECRET_ID` or both API Key arguments.

### Create and run the job

```bash
JOB=seeq-datalab-files-to-s3
aws s3 cp datalab-to-s3/seeq_datalab_files_to_s3.py s3://$BUCKET/scripts/seeq_datalab_files_to_s3.py

aws glue create-job --name $JOB --role arn:aws:iam::$ACCT:role/$ROLE \
  --command "Name=pythonshell,PythonVersion=3.9,ScriptLocation=s3://$BUCKET/scripts/seeq_datalab_files_to_s3.py" \
  --max-capacity 0.0625 --timeout 30 --max-retries 0 \
  --default-arguments "{\"--SEEQ_SECRET_ID\":\"$SECRET\",\"--S3_BUCKET\":\"$BUCKET\",\"--SOURCE_FOLDER\":\"files\",\"--library-set\":\"analytics\"}"

RUN=$(aws glue start-job-run --job-name $JOB \
  --arguments "{\"--DATALAB_URL\":\"$SEEQ_URL/data-lab/<PROJECT-ID>/lab\"}" \
  --query JobRunId --output text)
```

`0.0625` DPU (1/16) is the smallest Python shell size. Use `1` if you copy large files, because each file is held in
memory while it's copied. Then [wait for the run](#waiting-for-a-run-and-reading-its-logs) and check the results:

```bash
aws s3 ls s3://$BUCKET/seeq-datalab/ --recursive
```

### Run it locally

```bash
pip install requests boto3
python datalab-to-s3/seeq_datalab_files_to_s3.py \
  --DATALAB_URL "$SEEQ_URL/data-lab/<PROJECT-ID>/lab" \
  --S3_BUCKET $BUCKET --SEEQ_SECRET_ID $SECRET
```

### Limitations

- **Auth:** this uses an Application identity API Key for now. To switch to an IdP-issued token later, set `x-sq-auth`
  on the client session in place of calling `login()`. Nothing else changes.
- **Large files:** the contents API returns each file whole, as base64. For files of several GB, stream from
  `/data-lab/{id}/files/{path}` with `upload_fileobj` instead. That path hasn't been verified against Seeq's Data Lab
  proxy yet.
- **Incremental copies:** every run copies every file again. To skip unchanged files, compare `seeq-last-modified`
  against the existing S3 object's metadata.

---

## Use case 2: S3 CSV → Seeq signals

[`s3-to-seeq/s3_csv_to_seeq_signals.py`](s3-to-seeq/s3_csv_to_seeq_signals.py) reads a CSV from S3 and pushes each
column to Seeq as a signal with [SPy](https://pypi.org/project/seeq-spy/), inside a datasource named
**`AWS Glue S3 CSV Import`** (change it with `--DATASOURCE_NAME`).

### CSV layout

Wide: one timestamp column, then one column per signal.

```
Timestamp,Reactor Temperature,Reactor Pressure,Batch Phase
2026-11-01 00:00:00,71.2,101.3,Heating
2026-11-01 00:01:00,71.9,101.5,Heating
```

### How it works

1. Reads `s3://<S3_BUCKET>/<S3_KEY>` into a pandas DataFrame.
2. Uses the first column (or `--TIMESTAMP_COLUMN`) as the timestamp. Timestamps without an offset are read in
   `--TIMEZONE` (default `UTC`). Rows with unreadable timestamps are dropped, and if a timestamp appears twice the last
   row wins. Both cases are logged as warnings.
3. Every other column becomes a signal. If every non-empty cell in a column is a number it is a numeric signal;
   otherwise it is a string signal, created with the `string` unit. Empty cells are skipped.
4. Signs in with `spy.login(url=..., access_key=..., password=...)` using the Application identity API Key from
   Secrets Manager.
5. Calls `spy.push(data=..., metadata=..., datasource="AWS Glue S3 CSV Import", workbook=None)`. With no `--WORKBOOK`
   the signals are **globally scoped**, so they can be found from any workbook by searching the datasource. Pass
   `--WORKBOOK "AWS Glue >> S3 CSV Import"` to scope them to one workbook instead (that also adds a `From S3`
   worksheet).
6. Logs out. The job fails if any signal failed to push.

Running the job again with the same CSV, or with a new CSV that has the same column names, updates the **same** signals:
SPy derives each signal's Data ID from its name and datasource. Samples at the same timestamp are overwritten and new
timestamps are added. This was checked in testing: a second run created no duplicates.

### Job parameters

| Parameter                                          | Required | Default                  | Notes                                                              |
| -------------------------------------------------- | -------- | ------------------------ | ------------------------------------------------------------------ |
| `--SEEQ_URL`                                       | yes      |                          | e.g. `https://acme.seeq.site`                                      |
| `--S3_BUCKET`                                      | yes      |                          | Bucket that holds the CSV                                          |
| `--S3_KEY`                                         | yes      |                          | e.g. `seeq-import/reactor.csv`                                     |
| `--SEEQ_SECRET_ID`                                 | \*       |                          | Same secret as use case 1                                          |
| `--SEEQ_ACCESS_KEY` / `--SEEQ_ACCESS_KEY_PASSWORD` | \*       |                          | Alternative to the secret. Use for local testing only.             |
| `--DATASOURCE_NAME`                                | no       | `AWS Glue S3 CSV Import` | Datasource that holds the signals. Use it to manage access (ACLs). |
| `--SIGNAL_NAME_PREFIX`                             | no       |                          | Put in front of every signal name, e.g. `Reactor 1 `               |
| `--TIMESTAMP_COLUMN`                               | no       | first column             |                                                                    |
| `--TIMEZONE`                                       | no       | `UTC`                    | Only used for timestamps without an offset                         |
| `--UNITS`                                          | no       |                          | JSON map, e.g. `{"Reactor Temperature":"degC"}`                    |
| `--WORKBOOK`                                       | no       | (global scope)           | Workbook path to scope the signals to                              |
| `--VERIFY_SSL`                                     | no       | `true`                   |                                                                    |

\* Provide either `--SEEQ_SECRET_ID` or both API Key arguments.

### Installing SPy in Glue

`--additional-python-modules seeq~=66.0,seeq-spy` makes Glue `pip install` two packages at the start of each run (about
10 seconds):

- `seeq` is the Seeq REST SDK. `seeq-spy` does **not** include it. Its major version must match the Seeq server's
  (`R66.x` → `seeq~=66.0`). Check the server version at `https://<server>/api/system/server-status`.
- `seeq-spy` is SPy itself, which works with any recent server. Pin it (for example `seeq-spy==203.2`) for repeatable
  runs.

### Create and run the job

```bash
JOB=seeq-s3-csv-to-seeq
aws s3 cp s3-to-seeq/s3_csv_to_seeq_signals.py s3://$BUCKET/scripts/s3_csv_to_seeq_signals.py
aws s3 cp s3-to-seeq/sample_signals.csv s3://$BUCKET/seeq-import/sample_signals.csv

aws glue create-job --name $JOB --role arn:aws:iam::$ACCT:role/$ROLE \
  --command "Name=pythonshell,PythonVersion=3.9,ScriptLocation=s3://$BUCKET/scripts/s3_csv_to_seeq_signals.py" \
  --max-capacity 0.0625 --timeout 30 --max-retries 0 \
  --default-arguments "{\"--SEEQ_SECRET_ID\":\"$SECRET\",\"--S3_BUCKET\":\"$BUCKET\",\"--SEEQ_URL\":\"$SEEQ_URL\",\"--library-set\":\"analytics\",\"--additional-python-modules\":\"seeq~=66.0,seeq-spy\"}"

RUN=$(aws glue start-job-run --job-name $JOB \
  --arguments '{"--S3_KEY":"seeq-import/sample_signals.csv","--TIMEZONE":"America/Chicago","--UNITS":"{\"Reactor Temperature\":\"degC\",\"Reactor Pressure\":\"kPa\"}"}' \
  --query JobRunId --output text)
```

Use `--max-capacity 1` for large CSVs, because the whole file is held in memory. Then
[wait for the run](#waiting-for-a-run-and-reading-its-logs). A successful run logs:

```
INFO Read s3://.../seeq-import/sample_signals.csv (282 bytes)
INFO Parsed 3 signal(s) x 6 timestamp(s) from 2026-11-01 00:00:00-05:00 to 2026-11-01 00:05:00-05:00
INFO Logged in to https://<your-server>.seeq.site as <application identity> (SPy 203.2)
INFO Pushed to datasource 'AWS Glue S3 CSV Import':
                         Name                                   ID Push Result
          Reactor Temperature <ID>     Success
             Reactor Pressure <ID>     Success
                  Batch Phase <ID>     Success
INFO Done: 3 signal(s), 6 sample row(s) pushed from s3://.../seeq-import/sample_signals.csv
```

### Find the signals in Seeq

In Workbench, open the Data tab and search for `Reactor`, or filter by datasource **AWS Glue S3 CSV Import**. From
Python:

```python
from seeq import spy

spy.login(url='https://<your-server>.seeq.site', access_key='<KEY>', password='<PASSWORD>')
spy.search({'Datasource Name': 'AWS Glue S3 CSV Import'})
```

### Run it locally

```bash
pip install pandas boto3 "seeq~=66.0" seeq-spy
python s3-to-seeq/s3_csv_to_seeq_signals.py --SEEQ_URL $SEEQ_URL \
  --S3_BUCKET $BUCKET --S3_KEY seeq-import/sample_signals.csv --SEEQ_SECRET_ID $SECRET
```

### Limitations

- **Wide CSV only.** One timestamp column and one column per signal. A long/tall CSV (`timestamp, tag, value`) needs a
  `pivot` before the push.
- **One file per run.** To load every CSV under a prefix, loop over `list_objects_v2` and push each file.
- **Units** come only from `--UNITS`. They aren't parsed from the column headers. In testing, one signal kept `degC` as
  its source unit but showed a blank display unit in Seeq; the cause wasn't found.
- **A signal can't change type.** Once a signal holds numbers it can't hold text, and the other way round.

---

## Troubleshooting

| Symptom                                                       | Job | Cause / fix                                                                                                                                                                              |
| ------------------------------------------------------------- | --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `415 Unsupported Media Type` on `/api/auth/login`             | 1   | The login must send `Content-Type: application/vnd.seeq.v1+json`. The script already does this.                                                                                          |
| `Application identity API Key '...' was rejected` / login 401 | 1,2 | Wrong key or password, or the key has expired or been revoked. Create a new API Key for the application identity and update the secret.                                                  |
| `Access denied to Data Lab project` (403)                     | 1   | The project isn't shared with the Application identity API Key's user. Read access is enough.                                                                                            |
| `'files' not found` (404)                                     | 1   | The folder doesn't exist in the project. Check `--SOURCE_FOLDER`.                                                                                                                        |
| Job times out on the first request                            | 1   | The Data Lab project is starting, or the server isn't reachable from Glue (needs public egress).                                                                                         |
| `ModuleNotFoundError: No module named 'seeq'`                 | 2   | The job is missing `--additional-python-modules seeq~=66.0,seeq-spy`.                                                                                                                    |
| `No module named 'seeq.sdk'`                                  | 2   | `seeq-spy` is installed but the `seeq` SDK isn't. Add `seeq~=<server major>.0` to the modules list.                                                                                      |
| pip install of `seeq-spy` times out                           | 2   | The job has no internet egress (for example, a VPC connection without NAT). Add NAT, or ship wheels in S3.                                                                               |
| `... does not exist. Check --S3_BUCKET ...`                   | 2   | Wrong bucket or key, or the role lacks `s3:GetObject`.                                                                                                                                   |
| `None of the values in '...' could be read`                   | 2   | The first column isn't the timestamp. Set `--TIMESTAMP_COLUMN`.                                                                                                                          |
| Signals are off by some hours                                 | 2   | The CSV has timestamps without an offset that aren't UTC. Set `--TIMEZONE`, e.g. `America/Chicago`.                                                                                      |
| `Cannot change the value unit of measure from '' to 'string'` | 2   | A column that used to be numeric now holds text (or the signal was made by an older version of the script). Use a new column name or `--SIGNAL_NAME_PREFIX`, and archive the old signal. |
| `iam:PassRole` / `iam:CreateRole` AccessDenied                | all | Your AWS user can't create or pass roles in that account. Use a sandbox account or ask an AWS admin.                                                                                     |
| `InvalidParameterException ... logGroupName`                  | all | Git Bash rewrote `/aws-glue/...` into a Windows path. `export MSYS_NO_PATHCONV=1`.                                                                                                       |