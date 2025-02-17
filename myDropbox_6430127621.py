import json
import boto3
import uuid
import datetime

s3 = boto3.client("s3")

S3_BUCKET = "act5-dulyawat"
DEFAULT_USERNAME = "act4_default_user"

def upload_json_to_s3(key, data):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))

def put_file(event):
    file_name = event.get("file_name")
    file_content = event.get("file_content")

    if not file_name or not file_content:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing file_name or file_content"})
        }

    file_key = f"files/{DEFAULT_USERNAME}/{file_name}"

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=file_content)

        metadata_key = f"metadata/{DEFAULT_USERNAME}/{file_name}.json"
        metadata = {
            "file_name": file_name,
            "last_modified": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "owner": DEFAULT_USERNAME,
            "size": len(file_content),
        }
        upload_json_to_s3(metadata_key, metadata)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"File {file_name} uploaded"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }


def view_files():
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"metadata/{DEFAULT_USERNAME}/")
        files = []

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                metadata = s3.get_object(Bucket=S3_BUCKET, Key=key)
                file_data = json.loads(metadata["Body"].read().decode("utf-8")) 
                files.append(file_data)

        return {
            "statusCode": 200,
            "body": json.dumps({"files": files}) 
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)}) 
        }

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")

        match action:
            case "view":
                return view_files()
            case "put":
                return put_file(body)
            case _:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid action"})
                }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
