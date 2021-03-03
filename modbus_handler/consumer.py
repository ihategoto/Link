import json
from pystalk import BeanstalkClient, BeanstalkError

BEANSTALKD_HOST = '127.0.0.1'
BEANSTALKD_PORT = 11300

client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT, auto_decode = True)
client.watch("data")
while True:
    for job in client.reserve_iter():
        client.delete_job(job.job_id)
        print("{}".format(json.dumps(job.job_data)))