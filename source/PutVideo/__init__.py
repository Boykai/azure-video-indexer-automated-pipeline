import os
import logging
import requests
from azure.cosmosdb.table.tableservice import TableService
import azure.functions as func


def getViToken():
    '''
    '''
    try:
        # Attempt connection to VI for Access Token
        logging.info('Creating Video Indexer access token')

        # Format HTTPS Request
        headers = {'Ocp-Apim-Subscription-Key': os.environ['VI_KEY']}
        request_url = 'https://api.videoindexer.ai/Auth/{0}/Accounts/{1}/AccessToken?allowEdit=True'.format(
            os.environ['VI_LOCATION'], os.environ['VI_ACCOUNT_ID'])

        # Get Video Indexer access token
        response = requests.get(request_url,
                                headers=headers)
        response.raise_for_status()

        logging.info('Success: Created Azure Video Indexer access token')

        # Return Video Indexer access token
        return response.json()
    except Exception as e:
        logging.info(
            'Failed: Create Azure Video Indexer access token: {0}'.format(e))


def uploadVideo(access_token, video_url, video_name):
    '''
    '''
    try:
        # Attempt connection to VI for video upload
        logging.info(
            'Uploading video to Video Indexer - video: {0}'.format(video_name))

        # Format HTTPS Request
        headers = {'Content-Type': 'multipart/form-data'}
        params = {'privacy': 'Private',
                  'accessToken': access_token,
                  'videoUrl': video_url,
                  'callback_url': os.environ['VI_CALLBACK_URL'],
                  'priority': 'High'}

        request_url = 'https://api.videoindexer.ai/{0}/Accounts/{1}/Videos?name={2}'.format(
            os.environ['VI_LOCATION'], os.environ['VI_ACCOUNT_ID'], video_name)

        response = requests.post(request_url,
                                 headers=headers,
                                 params=params)

        logging.info('Success: Uploaded video to Video Indexer')

        return response.json()
    except Exception as e:
        logging.info('Failed: Upload video to Azure Video Indexer: {0} {1} {2}'.format(
            video_url, video_name, e))


def putTableEntity(video_id, video_name, video_path, video_url):
    '''
    '''
    try:
        # Get Azure Storage Table connection
        logging.info('Putting new entity to Azure Storage Table')

        table_service = TableService(
            connection_string=os.environ['SA_CONNX_STRING'])

        try:
            logging.info('Checking for existing Azure Storage Table')

            # Check for table, if none then create new table
            table_service.create_table(os.environ['SA_TABLE_TRACKER'],
                                       fail_on_exist=True)

            logging.info('Success: New table created')
        except Exception as e:
            logging.info('Failed: Table already exists {0}'.format(e))

        task = {
            'PartitionKey': 'examplekey',
            'RowKey': video_id,
            'VideoIndexerId': video_id,
            'VideoName': video_name,
            'VideoPath': video_path,
            'VideoUrl': video_url,
            'State': 'Uploaded'
        }

        # Write entry to Azure Storage Table
        table_service.insert_or_merge_entity(
            os.environ['SA_TABLE_TRACKER'], task)

        logging.info('Success: Put entity to Azure Storage Table')
    except Exception as e:
        logging.info('Failed: Put entity to Azure Storage Table {0}'.format(e))


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting...')

    # Get blob params
    sa_blob_path = req.params.get('path')
    sa_blob_name = req.params.get('name')
    sa_blob_uri = req.params.get('uri')

    # Get Video Indexer access token and upload video
    logging.info('Continuing .mp4 in file')

    vi_token = getViToken()
    vi_upload_response = uploadVideo(vi_token,
                                     sa_blob_uri,
                                     sa_blob_name)

    # Put new entity in Azure Blob Table tracker for Video Indexer
    vi_video_id = vi_upload_response['id']
    putTableEntity(vi_video_id, sa_blob_name, sa_blob_path, sa_blob_uri)

    logging.info('Completed.')

    return func.HttpResponse(
        '{0} uploaded to Azure Video Indexer'.format(sa_blob_name),
        status_code=200)
