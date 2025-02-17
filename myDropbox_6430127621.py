import json
import boto3
import datetime

s3 = boto3.client("s3")

S3_BUCKET = "act5-dulyawat"
DEFAULT_USERNAME = "act4_default_user"

# ========================================================================================================
def upload_json_to_s3(key, data):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))
    
def upload_metadata(file_name, size, username):
    metadata_key = f"metadata/{username}/{file_name}.json"
    metadata = {
        "file_name": file_name,
        "last_modified": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "owner": DEFAULT_USERNAME,
        "size": size,
    }
    upload_json_to_s3(metadata_key, metadata)
    
# ========================================================================================================
def put_file(event):
    file_name = event.get("file_name")
    file_content = event.get("file_content")

    if not file_name:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing file_name"})
        }

    file_key = f"files/{DEFAULT_USERNAME}/{file_name}"

    try:
        # write file
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=file_content)

        upload_metadata(file_name, len(file_content), DEFAULT_USERNAME)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"File {file_name} uploaded"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }

# ========================================================================================================
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
        
# ========================================================================================================
def get_file(event):
    file_name = event.get("file_name")

    if not file_name:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing file_name"})
        }

    file_key = f"files/{DEFAULT_USERNAME}/{file_name}"

    try:
        # get metadata
        response = s3.head_object(Bucket=S3_BUCKET, Key=file_key)
        content_type = response.get("ContentType", "application/octet-stream")

        # get file
        file_response = s3.get_object(Bucket=S3_BUCKET, Key=file_key)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": content_type, 
                "Content-Disposition": f'attachment; filename="{file_name}"'
            },
            "body": file_response["Body"].read(),
            "isBase64Encoded": True 
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }

# ========================================================================================================
def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")

        match action:
            case "view":
                return view_files()
            case "put":
                return put_file(body)
            case "get":
                return get_file(body)
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
