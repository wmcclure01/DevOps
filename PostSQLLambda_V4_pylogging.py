import requests
import json
import boto3
import logging

# Disable urllib3 warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_parameters(parameter_name):
    # Fetch parameters from AWS Systems Manager (SSM)
    ssm = boto3.client('ssm', 'us-east-2')
    response = ssm.get_parameters(Names=[parameter_name], WithDecryption=True)

    if not response.get('Parameters'):
        raise ValueError(f'No parameter found for name: {parameter_name}')

    parameters = json.loads(response['Parameters'][0]['Value'])

    # Log run details to CloudWatch
    logging.info(f'Run details: Parameter name: {parameter_name}, app_ids: {parameters.get("app_ids", [])}, '
                 f'Run-As ID: {parameters.get("credential_id")}, Run Priority: {parameters.get("priority")}')
    print(f'Run details: Parameter name: {parameter_name}, app_ids: {parameters.get("app_ids", [])}, '
          f'Run-As ID: {parameters.get("credential_id")}, Run Priority: {parameters.get("priority")}')

    return parameters

def authenticate(api_key, api_secret, auth_url):
    try:
        # Authenticate with Alteryx Gallery
        response = requests.post(
            auth_url,
            verify=False,
            data={
                'grant_type': 'client_credentials',
                'client_id': api_key,
                'client_secret': api_secret
            }
        )
        response_data = json.loads(response.text)
        token = response_data.get('access_token')
        
        if not token:
            raise ValueError('Authentication failed. Unable to retrieve access token.')
        
        logging.info('Authentication successful.')
        print('Authentication successful.')  # Add print statement
        return token

    except requests.exceptions.RequestException as e:
        # Handle authentication errors
        logging.error(f'Error during authentication: {e}')
        print(f'Error during authentication: {e}')  # Add print statement
        raise

def request_api(token, app_ids, credential_id=None, priority=None, base_url=None):
    success_responses = []
    error_responses = []
    headers = {'Authorization': 'Bearer ' + token}

    for app_id in app_ids:
        request_url = f'{base_url}/{app_id}/jobs'
        data = {}

        if credential_id:
            data['credentialId'] = credential_id
        if priority:
            data['priority'] = priority

        try:
            # Make API requests for each app ID
            response = requests.post(request_url, verify=False, headers=headers, data=data)

            if response.ok:
                success_responses.append(response.text)
                logging.info(f'Successful API request for app ID {app_id}.')
                print(f'Successful API request for app ID {app_id}.')  # Add print statement
            else:
                error_responses.append(response.text)
                logging.error(f'Error in API request for app ID {app_id}. Status code: {response.status_code}, Response: {response.text}')
                print(f'Error in API request for app ID {app_id}. Status code: {response.status_code}, Response: {response.text}')  # Add print statement

        except requests.exceptions.RequestException as e:
            # Handle API request errors
            logging.error(f'Error during API request for app ID {app_id}: {e}')
            print(f'Error during API request for app ID {app_id}: {e}')  # Add print statement
            raise

    return success_responses, error_responses

def send_to_google_chat(webhook_url, error_messages):
    if not error_messages:
        return  # No errors to send

    message = "\n".join(error_messages)

    payload = {
        "text": message
    }
    response = requests.post(webhook_url, json=payload)

    if response.ok:
        logging.info(f'Message sent to Google Chat: {message}')
        print(f'Message sent to Google Chat: {message}')  # Add print statement
    else:
        logging.error(f'Failed to send message to Google Chat. Status code: {response.status_code}')
        print(f'Failed to send message to Google Chat. Status code: {response.status_code}')  # Add print statement

def main(event, context):
    try:
        parameter_name = event.get('parameter_name')
        if not parameter_name:
            raise ValueError('No parameter name given in the event.')

        # Retrieve parameters from AWS SSM
        parameters = get_parameters(parameter_name)
        api_key = parameters.get('api_key')
        api_secret = parameters.get('api_secret')
        app_ids = parameters.get('app_ids', [])
        credential_id = parameters.get('credential_id')
        priority = parameters.get('priority')
        auth_url = parameters.get('auth_url')
        base_url = parameters.get('base_url')
        webhook_url = parameters.get('webhook_url')

        # Authenticate and make API requests
        token = authenticate(api_key, api_secret, auth_url)
        success_responses, error_responses = request_api(token, app_ids, credential_id, priority, base_url)

        for success_response in success_responses:
            logging.info(f'Success: {success_response}')
            print(f'Success: {success_response}')  # Add print statement

        if error_responses and webhook_url:
            # Send error messages to Google Chat if webhook_url is defined
            send_to_google_chat(webhook_url, error_responses)
            logging.warning('Some API requests failed. Check logs for details.')
            print('Some API requests failed. Check logs for details.')  # Add print statement

        return {'statusCode': 200, 'body': 'Success'}

    except Exception as e:
        # Handle any other exceptions
        logging.error(f'Error: {e}')
        print(f'Error: {e}')  # Add print statement
        if webhook_url:
            send_to_google_chat(webhook_url, [f'Error occurred: {e}'])
        return {'statusCode': 500, 'body': 'Internal Server Error'} 

if __name__ == '__main__':
    main()
