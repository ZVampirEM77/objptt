import os
import requests
import sys
import logging
import hashlib
import threading
from awsauth import S3Auth
from requests_aws4auth import AWS4Auth

g_logger = None
object_size = 4096
object_count = 10000

thread_count = 100

generate_log_path = "./log/"
generate_obj_path = "./test_obj/"
get_obj_path = "./get_obj/"

rgw_url = "http://127.0.0.1:8000"
access_key = "tester"
secret_key = "tester"

access_bucket = "test1"

authv2 = S3Auth('tester', 'tester', '127.0.0.1:8000')
authv4 = AWS4Auth('tester', 'tester', 'us', 's3')


def log_init(log_path = ''):
    logger = logging.getLogger("rgw_tracer")
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

    file_handler = logging.FileHandler(log_path + "rgw_tracer.log")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.setLevel(logging.INFO)

    return logger


def obj_generator(round_times, obj_no, obj_path = ''):
    content_unit = str(obj_no)
    content_unit_len = len(content_unit)
    fill_time = object_size / content_unit_len
    compensation_size = object_size % content_unit_len
    obj_handle = open(obj_path + str(round_times) + '_' + str(obj_no), 'w+')
    
    # fill content into obj
    while fill_time != 0:
        obj_handle.write(content_unit)
        fill_time -= 1

    # fill compensation content
    obj_handle.write(content_unit[:compensation_size])
    obj_handle.close()

def obj_cleaner(round_times, obj_no, obj_path = ''):
    os.remove(obj_path + str(round_times) + '_' + str(obj_no))

def generate_objdata_md5(obj_data):
    md5 = hashlib.md5(obj_data).hexdigest()
    return md5

def put_obj(bucket, obj, obj_path = ''):
    obj_handle = open(obj_path + obj, 'r')
    obj_data = obj_handle.read()
    url = rgw_url + '/' + bucket + '/' + obj
    res = requests.put(url, data = obj_data, auth = authv2)
    if res.status_code != 200 or res.text != '':
        g_logger.error("put_obj returns error, response status code = %d, response msg = %s"
                       % (res.status_code, res.text))
        return -1
    return 0

def get_obj(bucket, obj, obj_path = ''):
    url = rgw_url + '/' + bucket + '/' + obj
    res = requests.get(url, auth = authv2)
    if res.status_code != 200:
        g_logger.error("get_obj returns error, response status code = %d, response msg = %s"
                       % (res.status_code, res.text))
        return -1

    local_obj_handle = open(obj_path + obj, 'w+')
    local_obj_handle.write(res.text)
    local_obj_handle.close()
    return 0

def verify_objdata_md5(orig_obj, dest_obj, orig_obj_path = '', dest_obj_path = ''):
    orig_obj_handle = open(orig_obj_path + orig_obj, 'r')
    orig_obj_data = orig_obj_handle.read()
    orig_obj_md5 = generate_objdata_md5(orig_obj_data)

    dest_obj_handle = open(dest_obj_path + dest_obj, 'r')
    dest_obj_data = dest_obj_handle.read()
    dest_obj_md5 = generate_objdata_md5(dest_obj_data)

    if orig_obj_md5 != dest_obj_md5:
        g_logger.error("verify_objdata_md5 orig_obj_md5 != dest_obj_md5, orig_obj_md5 = %s, \
                        dest_obj_md5 = %s " % (orig_obj_md5, dest_obj_md5))
        return False
    else:
        return True
        

def workload(thread_id):
    round_times = 10
    
    while round_times != 0:
        for obj_no in range(1, object_count + 1):
            obj_number = obj_no * thread_count + thread_id
            obj_generator(round_times, obj_number, generate_obj_path)
            if put_obj(access_bucket, str(round_times) + '_' + str(obj_number), generate_obj_path) != 0:
                return -1 
    
        for obj_no in range(1, object_count + 1):
            obj_number = obj_no * thread_count + thread_id
            if get_obj(access_bucket, str(round_times) + '_' + str(obj_number), get_obj_path) != 0:
                return -1
    
            if verify_objdata_md5(str(round_times) + '_' + str(obj_number), str(round_times) + '_' + str(obj_number), generate_obj_path, get_obj_path) == False:
                g_logger.error("verify_objdata_md5 return False!")
                return -1
    
            try:
                obj_cleaner(round_times, obj_number, generate_obj_path)
                obj_cleaner(round_times, obj_number, get_obj_path)
            except OSError :
                g_logger.error("obj_cleaner trys to clean non-exist object, obj_number = %s" % (str(obj_number)))
                return -1

        round_times -= 1

    g_logger.info("Everything is OK! thread_id = %d" % (thread_id))

    return 0


def main():
    global g_logger
    g_logger = log_init(generate_log_path)

    for tele in range(1, thread_count + 1):
        test_thread = threading.Thread(target = workload, args = (tele,))
        test_thread.start()
    

if __name__ == "__main__":
    sys.exit(main())
