#################################
#################################
#################################
#################################



##############################################################
############### Alteryx API Load Jobs to Queue ###############
############### Alteryx Workflow Version  ####################
############### Author: Wayne McClure ########################
############### Version: 0.8 #################################
############### Last Update: 03/14/2023 ######################
##############################################################
# List all non-standard packages to be imported by your 
# script here (only missing packages will be installed)
from ayx import Package
#Package.installPackages(['pandas','numpy'])


#################################
from ayx import Alteryx
import requests
import json
import urllib3
urllib3.disable_warnings() #SSL cert verification is turned off since closed loop requests within VPC



#################################
##############################################
############### AYX Check ###############
##############################################
def ayx_read():
  
    try:
        df = Alteryx.read("#1")
        CanReadDataset = 1
    except:
        CanReadDataset = 0
    finally:
        pass

    if CanReadDataset == 1:
        for i in df['flag']:
            if i == 0:
                results = 'true'
                Alteryx.write(df, 1)
            else:
                results = 'false'
    else:
        results = 'false'
    return results





#################################
##############################################
############### GET AUTH TOKEN ###############
##############################################
def authenticate(results):

    if results == 'true' :
# Auth variables
        apiKey = '8DB0AC0CA383FC3e80432cba88f35dce7c2e48c06976f99906b7272a86519fb980d14d5cc594ec9'
        apiSecret = 'f454f42f9d9adf3d654a44f7b0cd64fa3e5a7cc0990c51889dd3593f782cd49c'
        authUrl = 'https://alteryx-gallery.oa-np.nlsn.media/webapi/oauth2/token'

# Request auth token
        response = requests.post(
            authUrl, 
            verify=False,
            data = {
                'grant_type':'client_credentials',
                'client_id' : apiKey,
                'client_secret' : apiSecret
            }
        )

# Parse the access token
        response = json.loads(response.text)
        token = response['access_token']

        return token
    else:
        token = '0'
        return token


#################################
#############################################
###### MAKE A REQUEST TO THE SERVER API ######
##############################################
def requestapi(token):
    appID = ["63e418ae25159137fc1c1b4f","63ebb233aa7a4172dd82830e","63ebb21caa7a4172dd8282d9"] #list of all jobIDs associated with SNS trigger
    
    if token == 0 :
        return
    else:
    
        headers = {'Authorization' : 'Bearer ' + token}
    #loop to push request for each item in appID list

        for y in appID :
            requestUrl = f'https://alteryx-gallery.oa-np.nlsn.media/webapi/v1/workflows/{y}/jobs'
            postJobs = requests.request('POST',
                                requestUrl,
                                verify=False,
                                headers=headers)

    #postJobs = json.loads(collections.text) ##commented out function that returns success ****TODO to push results to Postgres table




#################################
#############################################
###############Lambda Init###############
##############################################

def main(): #Constructs main function
    b= ayx_read()
    d= authenticate(b)
    q = requestapi(d)
    
    return q



#################################
if __name__ == '__main__': #initialize 
    main()



#################################


#################################
