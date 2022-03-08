from importlib.abc import Loader
import yaml
from threading import *
import datetime
import time
import csv
from yaml import Loader

def TimeFunction(seconds):
    time.sleep(seconds)

def entry_log(k):
    with open("Milestone2A_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) + ";" + str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=Loader)
        return data
    
file = "DataSet/Milestone2/Milestone2A.yaml"

data = load_data(file)
data = data['M2A_Workflow']

txt = "M2A_Workflow"

tasks = []
dict = {}

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
                # if 'Condition' in v:
                #     cond = v['Condition'].split(' ')
                #     key = cond[0]
                #     val = cond[2]
                #     if not key in dict:
                #         continue
                #     v1 = int(dict[key])
                #     v2 = int(val)
                #     print(v1, v2)
                #     if int(dict[key]) >= int(val):
                #         entry_log(txt + " Skipped")
                #         continue
                #     elif cond[1] == '>' and int(dict[key]) <= int(val):
                #         entry_log(txt + " Skipped")
                #         continue
                find_task(txt + "." + k ,v)
            #     t = Thread(target=find_task, args=(txt + "." + k ,v,))
            #     thread_items.append(t)
            # for t in thread_items:
            #     t.start()
            # for t in thread_items:
            #     t.join()
    elif data['Type'] == 'Task':
        func = data['Function']
        
        if func == "TimeFunction":
            finput = data['Inputs']['FunctionInput']
            ftime = data['Inputs']['ExecutionTime']
            if "Condition" in data:
                cond = data['Condition'].split(' ')
                key = cond[0]
                entry_log(txt + " Executing " + str(func) + " (" + str(dict[key]) + ", " + str(ftime) + ")")
            else:
                entry_log(txt + " Executing " + str(func) + " (" + str(finput) + ", " + str(ftime) + ")")
            TimeFunction(int(ftime))
        elif func == "DataLoad":
            fname = data['Inputs']['Filename']
            filepath = "DataSet/Milestone2/" + fname
            entry_log(txt + " Executing " + str(func)+" ("+fname+")")
            with open(filepath,"r")as f:
                csvdata = csv.reader(f)
                nofdefect = 0
                for data in csvdata:
                    nofdefect += 1
                keyn = "$(" + txt + ".NoOfDefects" + ")"
                dict[keyn] = nofdefect
        tasks.append(data)
    entry_log(txt + " Exit")


find_task(txt, data)

print(dict)
# print(tasks)
        
        