import boto3
import os
import csv

aws_access_key = os.getenv('AWS_KEY')
aws_secret_key = os.getenv('AWS_SECRET')
file_path = os.getenv('FILE_PATH')
s3_bucket_name = os.getenv('S3_BUCKET')

replacements = {}

with open(file_path, 'rt')as f:
    data = csv.reader(f)
    for row in data:
        replacements[row[2]] = row[3]

aws = boto3.session.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

s3 = aws.resource('s3')
s3_bucket = s3.Bucket(s3_bucket_name)

for obj in s3_bucket.objects.all():
    key = obj.key
    path = key.split('/')
    cid = path[1]
    if not cid.isnumeric():
        continue

    path[1] = replacements[cid]
    new_path = '/'.join(path)
    s3.Object(s3_bucket_name, new_path).copy_from(CopySource=s3_bucket_name + '/' + key)
    s3.Object(s3_bucket_name, key).delete()
    print('migrated: ' + key + '->' + new_path)

