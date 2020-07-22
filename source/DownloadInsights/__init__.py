import os
import json
import logging
import requests
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import BlobServiceClient
import azure.functions as func


def getTableEntity(entity_id):
    '''
    '''
    try:
        # Get subset of feature values for rows with State not 'Processed'
        logging.info('Getting Azure Storage Table entity')

        # Get Azure Storage Table connection
        table_service = TableService(
            connection_string=os.environ['SA_CONNX_STRING'])

        tasks = table_service.query_entities(os.environ['SA_TABLE_TRACKER'],
                                             filter="VideoIndexerId eq '{0}'".format(
                                                 entity_id),
                                             select='RowKey, PartitionKey, VideoIndexerId, VideoName, VideoPath, VideoUrl')

        entity = [task for task in tasks][0]

        return entity
    except Exception as e:
        logging.info('Get Table Video List failed: {0}.'.format(e))


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


def getArtifact(access_token, video_id, artifact_type):
    '''
    '''
    try:
        # Attempt connection to VI for video JSON artifact
        logging.info(
            'Getting Video Indexer {0} artifact'.format(artifact_type))

        # Format HTTPS Request
        params = {'accessToken': access_token}
        request_url = 'https://api.videoindexer.ai/{0}/Accounts/{1}/Videos/{2}/ArtifactUrl?type={3}'.format(
            os.environ['VI_LOCATION'], os.environ['VI_ACCOUNT_ID'], video_id, artifact_type)

        # Get Video Indexer video JSON artifact
        response = requests.get(request_url,
                                params=params)
        response.raise_for_status()

        logging.info(
            'Success: Video Indexer {0} artifact returned'.format(artifact_type))

        # Return Video Indexer JSON artifact
        return requests.get(response.json()).json()
    except Exception as e:
        logging.info('Failed: Get Video Indexer artifact - id: {0} artifact_type: {1} {2}'.format(
            video_id, artifact_type, e))
        return dict()


def putBlob(file_path, sa_container):
    '''
    '''
    try:
        # Attempt to put URL to Azure Blob Storage
        logging.info('Putting file to Azure Blob Storage')

        # Create a blob client using the local file name as the name for the
        # blob
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ['SA_CONNX_STRING'])
        blob_client = blob_service_client.get_blob_client(
            container=sa_container, blob=file_path)

        # Upload the created file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        logging.info('Success: Put file to Azure Blob Storage')
    except Exception as e:
        logging.info('Failed: Put file to Azure Blob Storage {0}'.format(e))


def getInsights(access_token, video_id):
    '''
    '''
    try:
        # Attempt connection to VI for video JSON insights
        logging.info('Getting Video Indexer insights')

        # Format HTTPS Request
        headers = {'Ocp-Apim-Subscription-Key': os.environ['VI_KEY']}
        params = {'accessToken': access_token}
        request_url = 'https://api.videoindexer.ai/{0}/Accounts/{1}/Videos/{2}/Index'.format(
            os.environ['VI_LOCATION'], os.environ['VI_ACCOUNT_ID'], video_id)

        # Get Video Indexer video JSON insights
        response = requests.get(request_url,
                                headers=headers,
                                params=params)
        response.raise_for_status()

        logging.info('Success: Video Indexer insights returned')

        # Return Video Indexer JSON insights
        return response.json()
    except Exception as e:
        logging.info('Failed: Get Azure Video Indexer insights {0}'.format(e))


def putTableEntity(video_id, video_name, video_path, insights_path, video_url):
    '''
    '''
    try:
        logging.info('Putting new entity to Azure Storage Table')

        # Get Azure Storage Table connection
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
            'InsightsPath': insights_path,
            'State': 'Processed'
        }

        # Write entry to Azure Storage Table
        table_service.insert_or_merge_entity(
            os.environ['SA_TABLE_TRACKER'], task)

        logging.info('Success: Put entity to Azure Storage Table')
    except Exception as e:
        logging.info('Failed: Put entity to Azure Storage Table {0}'.format(e))


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting...')

    # Get HTTPS request params
    vi_video_id = req.params.get('id')

    # Get Azure Storage Table tracker params
    tracker_table = getTableEntity(vi_video_id)
    sa_video_url = tracker_table['VideoUrl']
    sa_video_path = tracker_table['VideoPath']
    video_name = tracker_table['VideoName']

    # Get Video Indexer JSON artifacts, store in Azure Blob
    vi_artifacts = ['Ocr',
                    'Faces',
                    'VisualContentModeration',
                    'TextualContentModeration',
                    'LanguageDetection',
                    'MultiLanguageDetection',
                    'Metadata',
                    'Emotions',
                    'TextualContentModeration']

    vi_token = getViToken()
    sa_container = sa_video_path.split('/')[0]
    sa_video = '/'.join(sa_video_path.split('/')[1:])

    # For each artifact type, write JSON to Azure Blob Storage
    for vi_artifact in vi_artifacts:
        vi_artifact_path = '{0}_{1}.json'.format(os.path.splitext(sa_video)[0],
                                                 vi_artifact)

        # Get Video Indexer JSON artifact
        vi_artifact_json = getArtifact(vi_token, vi_video_id, vi_artifact)

        # Save Video Indexer JSON artifact, local
        with open(vi_artifact_path, 'w') as f:
            json.dump(vi_artifact_json, f)

        # Upload Video Indexer JSON artifact to Azure Blob Storage
        putBlob(vi_artifact_path, sa_container)

        # Delete local artifact file
        os.remove(vi_artifact_path)

    # Get Video Indexer Insights JSON to save to Azure Blob Storage
    vi_insights_json = getInsights(vi_token, vi_video_id)

    # Save Video Indexer JSON artifact, local
    vi_insights_path = '{0}_Insights.json'.format(video_name)

    # Save Video Indexer JSON insights, local
    with open(vi_insights_path, 'w') as f:
        json.dump(vi_insights_json, f)

    # Upload Video Indexer Insights JSON to Azure Blob Storage
    putBlob(vi_insights_path, sa_container)

    # Delete local artifact file
    os.remove(vi_insights_path)

    # Update Azure Storage tracking table
    putTableEntity(
        vi_video_id,
        video_name,
        sa_video_path,
        vi_insights_path,
        sa_video_url)

    logging.info('Completed.')

    return func.HttpResponse(
        'Success: Video Indexer JSON uploaded to Azure Blob Storage',
        status_code=200)
