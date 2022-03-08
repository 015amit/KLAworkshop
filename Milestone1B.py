from concurrent.futures import thread
from importlib.abc import Loader
import yaml
import threading
import datetime
import time
# from yaml import Loader

# def TimeFunction(seconds):
#     time.sleep(seconds)

def entry_log(k):
    with open("Milestone1B_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) + ";" + str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=yaml.FullLoader)
        return data
    
file = "DataSet/Milestone1/Milestone1B.yaml"

data = load_data(file)
data = data['M1B_Workflow']

txt = "M1B_Workflow"
lock = threading.Lock()
tasks = []

def find_task(txt,data):
    entry_log(txt + " Entry ")
    if data['Type'] == 'Flow':
        execution = data['Execution']
        activities = data['Activities']
        if execution == 'Sequential':
            for k, v in activities.items():
                find_task(txt + "." + k ,v)
        elif execution == 'Concurrent':
            threads_items = []
            for k, v in activities.items():
                t = threading.Thread(target=find_task, args=(txt + "." + k ,v,))
                threads_items.append(t)
            for t in threads_items:
                t.start()
            for t in threads_items:
                t.join()
        entry_log(txt +  " Exit ")
    elif data['Type'] == 'Task':
        fname = data['Function']
        finput = data['Inputs']['FunctionInput']
        ftime = data['Inputs']['ExecutionTime']
        entry_log(txt + " Executing " + str(fname) + " ("+ str(finput)+ ", "+str(ftime)+ ")")
        time.sleep(int(ftime))
        entry_log(txt +  " Exit ")
    
        # tasks.append(data)
    

find_task(txt, data)

        
        