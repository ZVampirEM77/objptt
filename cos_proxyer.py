import requests
import sys
import logging
import time
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

g_logger = None

def log_init():
    logger = logging.getLogger("rgw_tracer")
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

    file_handler = logging.FileHandler("rgw_tracer.log")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)

    return logger

def update_workload_config(file_path, oprefix):
    tree = ET.ElementTree(file=file_path)
    for elem in tree.iterfind('workflow/workstage/work/operation'):
        config_dict = dict([config_elem.split('=') for config_elem in elem.attrib['config'].split(';')]) 
        g_logger.debug("update_workload_config old oprefix = " + config_dict['oprefix'])
        config_dict['oprefix'] = oprefix
        g_logger.debug("update_workload_config new oprefix = " + config_dict['oprefix'])
        tmp_list = []
        for k, v in config_dict.items():
            tmp_list.append(k + '=' + v)
        elem.attrib['config'] = ';'.join(tmp_list)
        g_logger.debug("new operation config field is " + elem.attrib['config'])
    tree.write(file_path, encoding="UTF-8", xml_declaration=True)
    

def get_workload_processing_info(workload_id):
    url = "http://127.0.0.1:19088/controller/cli/index.action?id=" + workload_id
    res = requests.get(url)
    if res.status_code != 200:
        g_logger.error("get_workload_processing_info returns status code " + str(res.status_code))
        return -1, False
    return 0, "Total: 0 active workloads" in res.content

def submit_workload():
    url = "http://127.0.0.1:19088/controller/cli/submit.action"
    fhandle = open("./cos/conf/blkin-test.xml", 'rb')
    workload_conf = fhandle.read()
    res = requests.post(url, files = {'config': workload_conf})
    if res.status_code != 200 or "Accepted with ID" not in res.text:
        g_logger.error("submit_workload returns status code " + str(res.status_code))
        g_logger.error("submit workload returns response text " + res.text)
        return -1, ""
    workload_id = res.text.split(":")[1][1:]
    return 0, workload_id

def do_one_work():
    submit_ret, workload_id = submit_workload()
    if submit_ret != 0:
        return -1

    query_ret, finished = get_workload_processing_info(workload_id)
    while query_ret == 0 and finished == False:
        time.sleep(5)
        query_ret, finished = get_workload_processing_info(workload_id)

    if query_ret == 0:
        g_logger.info( workload_id + " has been processed successfully")
    else:
        g_logger.info( workload_id + " has been processed failed")
    return 0
    

def main():
    global g_logger
    g_logger = log_init()
    work_count = 4

    while work_count != 0:
        update_workload_config('./cos/conf/blkin-test.xml', 'em_test' + str(work_count))
        do_ret = do_one_work()
        if do_ret != 0:
            return do_ret 
        work_count -= 1
#    update_workload_config('./cos/conf/blkin-test.xml', 'em_test' + str(work_count))
    return 0


if __name__ == '__main__':
    sys.exit(main()) 
