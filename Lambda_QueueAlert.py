#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
from pymongo import MongoClient
import datetime
import requests
from sqlalchemy import create_engine
from datetime import date, timedelta



# In[8]:


def authenticate():  
    client = MongoClient('PASS')
    db = client.AlteryxService
    collection = db.AS_Queue
    start_of_day = date.today().strftime("%Y-%m-%d")
    end_of_day = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    doc_filter = {
        "CreationDateTime": {
            '$gte': start_of_day,
            '$lt': end_of_day
            }
        } 
    data = pd.DataFrame(list(collection.find(doc_filter,{"IsAnonymous":0,"RestrictOutput":0,"AS_Schedules__ID":0,"__ServiceData":0,"__Version":0,"__UpdateCheck":0})))
   
   
    data = data.astype('string')

    #Change time to D/T
    data["CompletionDateTime"] = pd.to_datetime(data["CompletionDateTime"])
    data["CreationDateTime"] = pd.to_datetime(data["CreationDateTime"])
    '''  
    #Grab only todays data
    today = datetime.datetime.now().strftime('%Y/%m/%d')
    data = data[data['CreationDateTime'] >= today]
    #normalize to postgres output '''
    data.columns = map(str.lower, data.columns)

    return data


# In[9]:


def queue_check(data):

    #Creates second DF to look at last 4 hour results of queue table
    time_now= datetime.datetime.now()
    data_last_hour = data[data.creationdatetime >= time_now - pd.Timedelta('4H')]

    #Remove nulls and adjust to find worker nodes
    data_last_hour= data_last_hour[data_last_hour['owner'].str.contains("IP")]
    #Splits by delimiter
    data_last_hour = data_last_hour.owner.str.split('|').apply(pd.Series)

    #trims to columns of split and renames
    data_last_hour.columns = ["worker_nodes","misc"]
    data_last_hour= pd.concat([data_last_hour])

    #Variables we will pass into the output function creating the return string if !3 of workers_count + workers_listed
    workers_count=  data_last_hour["worker_nodes"].nunique()
    workers_listed = data_last_hour["worker_nodes"].unique()


    #Now we compare the history of the queue
    #reuse the time_now variable to check the number of minutes since last 
    pd.options.mode.chained_assignment = None
    queue_check = data[data["status"] == "Queued"]
    queue_check['time_now'] = time_now.strftime("%m/%d/%Y, %H:%M:%S")
    queue_check['time_diff']= pd.Series(dtype='int')
    queue_check['time_now']= pd.to_datetime(queue_check['time_now'])
    queue_check['time_diff'] = (queue_check.time_now -queue_check.creationdatetime)
    queue_check['time_diff'] = (queue_check['time_diff'].dt.total_seconds().div(60).astype(int))

 ##Create output table for reporting with running jobs to look for likely culprits.
    running_jobs = data[data["status"] == "Running"]
    running_jobs['run_date'] = time_now.strftime("%m/%d/%Y")
    running_jobs['alert_color']= pd.Series(dtype='str')
    running_jobs['run_date']= pd.to_datetime( running_jobs['run_date'])
    #Grab largest and total queue
    max_queue = queue_check['time_diff'].max()
    queue_count = queue_check['_id'].nunique()
## Creates the variables containing results
    noderesults= "Number of nodes: " + str(workers_count) + " Nodes running: " + workers_listed
    queueresults= "Oldest job in Queue is: " + str(max_queue) + " minutes. Number of jobs in Queue: " + str(queue_count)
    
##Check Logic 

    if (workers_count != 3 and (time_now.hour < 0 or time_now.hour > 7))   :
        worker_results = 100
    else:
        worker_results = 0
    if max_queue > 45:
        queue_results = 200
    else:
        queue_results = 0
        
    engine = create_engine('postgresql:<PASS>')

    #running_jobs.to_sql('mongo_py_alert', engine,schema='usage', if_exists='replace',index=False)
      ##commented out print to correct for pass along to other functions

    if (worker_results + queue_results) == 300:
        #print("Situation RED: " +  noderesults + " " + queueresults)
        message = "Situation RED: " +  noderesults + " " + queueresults
        running_jobs['alert_color']= "RED"
        running_jobs.to_sql('mongo_temp', engine,schema='usage', if_exists='append',index=False)
    elif worker_results + queue_results == 200:
        #print("Situation Yellow: " + queueresults)
        message = "Situation Orange: " + queueresults
        running_jobs['alert_color']= "ORANGE"
        running_jobs.to_sql('mongo_temp', engine,schema='usage', if_exists='append',index=False)
    elif  worker_results + queue_results == 100:
        #print("Situation Orange: " + noderesults)
        message = "Situation Yellow: " + noderesults
        running_jobs['alert_color']= "YELLOW"
        running_jobs.to_sql('mongo_temp', engine,schema='usage', if_exists='append',index=False)
        
   
    else:
        running_jobs['alert_color']= "GREEN"
        message = None
    return message


# In[10]:


####url = "https://hooks.slack.com/services/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"
def send_slack_message(message: str):
    '''
    This function contains the logic to send a message to slack
    '''
    if message is None:
        print("situation normal")
    else:
        payload = '{"text": "%s"}' %message
        response = requests.post('https://hooks.slack.com/services/<HOOKID>',
                             data = payload)
    
    


# In[11]:


def main(event, context):
    d= authenticate()
    q = queue_check(d)
    m = send_slack_message(q)
    return q


# In[12]:



if __name__ == '__main__':
    main()


# In[ ]:




