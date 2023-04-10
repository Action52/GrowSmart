import boto3

session = boto3.Session(region_name='eu-west-3')



s3 = session.resource('s3')

my_bucket = s3.Bucket('growsmarttemporallanding')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)
