import boto3

min=0
max = 105000000
glue = boto3.client('glue', 'my-region-name')
while min< max:
    minL=str(min)
    min= min+7000000
    minU=str(min)
    args = {'--rangemin':minL,'--rangemax':minU}
    glue.start_job_run(JobName='my-job-name',Arguments=args)
print("done")
'''
starting multiple instances of same job with different input parameters
'''
