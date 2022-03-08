from importlib.abc import Loader
import yaml
from threading import *
import datetime
import time
from yaml import Loader

def TimeFunction(seconds):
    time.sleep(seconds)

def entry_log(k):
    with open("Milestone1A_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) + ";" + str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=Loader)
        return data
    
file = "DataSet/Milestone1/Milestone1A.yaml"

data = load_data(file)
data = data['M1A_Workflow']

txt = "M1A_Workflow"

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
            thread_items = []
            for k, v in activities.items():
                t = Thread(target=find_task, args=(txt + "." + k ,v,))
                thread_items.append(t)
            for t in thread_items:
                t.start()
            for t in thread_items:
                t.join()
    elif data['Type'] == 'Task':
        func = data['Function']
        finput = data['Inputs']['FunctionInput']
        ftime = data['Inputs']['ExecutionTime']
        entry_log(txt + " Executing " + str(func) + " (" + str(finput) + ", " + str(ftime) + ")")
        if func == "TimeFunction":
            TimeFunction(int(ftime))
        tasks.append(data)
    entry_log(txt + " Exit")

find_task(txt, data)

# print(tasks)
        
        