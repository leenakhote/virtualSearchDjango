#
# import boto3
#
# bucket = "darwin-vs"
# file_name = "train_no.csv"
#
# s3 = boto3.client(
#     's3',
#     aws_access_key_id = 'AKIAJ6IIHKANYPNE5X6A',
#     aws_secret_access_key= '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'
# )
# obj = s3.get_object(Bucket= bucket, Key= file_name)
# print obj['Body'].read()
#


import boto3
s3 = boto3.resource('s3')
#
#
s3.meta.client.upload_file('/home/leena/dev/visualsearch/models/train_no.csv', 'darwin-vs', 'train_no1.csv')     #--------  Writing csv   -------
#
