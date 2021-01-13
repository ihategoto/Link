from pystalk import BeanstalkClient

client = BeanstalkClient('127.0.0.1', 11300)
client.watch("data")
while True:
    for job in client.reserve_iter():
        client.delete_job(job.job_id)
        print(job.job_data)