"""R14: Lambda handlers for the serverless batch path (ADR-002).

Three entry points over one codebase, reusing pipelines/normalize.py:
  validate   S3 landing object -> schema check -> bronze/ or quarantine/ + SNS
  normalize  bronze object -> canonical rows -> silver/ (ndjson for Iceberg)
  flag       silver rows -> exception rules -> exceptions/ + SNS per new exception

Packaged by infra/terraform (zip of lambdas/ + pipelines/ + india/ + config/).
"""

import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

from pipelines.normalize import detect_exceptions, normalize_event

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET = os.environ.get("LAKE_BUCKET", "")
ALERT_TOPIC = os.environ.get("ALERT_TOPIC_ARN", "")


def _object_from_event(event: dict) -> tuple[str, str]:
    rec = event["Records"][0]
    body = json.loads(rec["body"]) if "body" in rec else rec  # SQS-wrapped or direct
    s3rec = body["Records"][0]["s3"] if "Records" in body else body["detail"]
    bucket = s3rec["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        s3rec["object"]["key"] if "object" in s3rec else s3rec["requestParameters"]["key"])
    return bucket, key


def _notify(subject: str, message: str) -> None:
    if ALERT_TOPIC:
        sns.publish(TopicArn=ALERT_TOPIC, Subject=subject[:99], Message=message)


def validate(event, context):
    bucket, key = _object_from_event(event)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    try:
        for line in body.splitlines():
            json.loads(line)
    except json.JSONDecodeError as e:
        qkey = key.replace("landing/", "quarantine/", 1)
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=qkey)
        s3.delete_object(Bucket=bucket, Key=key)
        _notify("manifest: file quarantined", f"s3://{bucket}/{qkey}: {e}")
        return {"status": "quarantined", "key": qkey}
    bkey = key.replace("landing/", "bronze/", 1)
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=bkey)
    s3.delete_object(Bucket=bucket, Key=key)
    return {"status": "landed", "key": bkey}


def normalize(event, context):
    bucket, key = _object_from_event(event)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    rows, quarantined = [], []
    for line in body.splitlines():
        row, reason = normalize_event(json.loads(line))
        if reason:
            quarantined.append(dict(json.loads(line), reason=reason))
        else:
            row.pop("_lastmile", None)
            rows.append(row)
    out_key = key.replace("bronze/", "silver/events/", 1)
    s3.put_object(Bucket=bucket, Key=out_key,
                  Body="\n".join(json.dumps(r) for r in rows))
    if quarantined:
        qkey = key.replace("bronze/", "quarantine/rows/", 1)
        s3.put_object(Bucket=bucket, Key=qkey,
                      Body="\n".join(json.dumps(q) for q in quarantined))
        _notify("manifest: rows quarantined",
                f"{len(quarantined)} rows -> s3://{bucket}/{qkey}")
    return {"rows": len(rows), "quarantined": len(quarantined), "key": out_key}


def flag(event, context):
    bucket, key = _object_from_event(event)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    rows = [json.loads(l) for l in body.splitlines()]
    excs = detect_exceptions(rows, datetime.now(timezone.utc))
    out_key = key.replace("silver/events/", "silver/exceptions/", 1)
    s3.put_object(Bucket=bucket, Key=out_key,
                  Body="\n".join(json.dumps(e) for e in excs))
    for e in excs:
        _notify(f"manifest exception: {e['exception_type']}",
                json.dumps(e, indent=2))
    return {"exceptions": len(excs), "key": out_key}
