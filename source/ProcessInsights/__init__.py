import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.cosmosdb.table.tableservice import TableService
import azure.functions as func


def getInsightsBlobsLocal(blob_container='content'):
    '''
    '''
    try:
        # Get all Insights blobs to local files
        logging.info('Getting Azure Storage blobs to local files')

        # Connect to Blob Client to get list of blobs with 'Insights' in name
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ['SA_CONNX_STRING'])
        container_client = blob_service_client.get_container_client(
            blob_container)

        # Get list of Insights blobs
        blobs_list = container_client.list_blobs()
        insight_blobs = [
            blob for blob in blobs_list if 'Insights' in blob.name]

        # Download blobs to local storage and return file paths
        local_files = []
        for blob in insight_blobs:
            blob_client = container_client.get_blob_client(blob.name)
            local_file = blob.name.split('/')[-1]
            with open(local_file, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
            local_files.append(local_file)

        logging.info('Success: Azure Storage blobs downloaded to local files')

        return local_files
    except Exception as e:
        logging.info('Failed: Put Azure Storage blob to file {0}'.format(e))


def getFeature(data, feature, feature_type='text'):
    '''
    '''
    try:
        return [{item[feature_type]: item['confidence']}
                for item in data[feature]]
    except BaseException:
        return {}


def getLabels(data, feature):
    '''
    '''
    results = []
    for item in data[feature]:
        item_confidence = [instance['confidence']
                           for instance in item['instances']]
        results.append({item['name']: max(item_confidence)})

    return results


def mergeInsights(file_list):
    '''
    '''
    merged_insights = []

    for files in file_list:
        video_insights = list(pd.read_json(files)['insights'])[0]

        # Get video features from Insights JSON
        video_features = {'brands': getFeature(video_insights,
                                               'brands',
                                               'name'),
                          'topics': getFeature(video_insights,
                                               'topics',
                                               'name'),
                          'keywords': getFeature(video_insights,
                                                 'keywords'),
                          'labels': getLabels(video_insights,
                                              'labels'),
                          'ocr': getFeature(video_insights,
                                            'ocr'),
                          'namedLocations': getFeature(video_insights,
                                                       'namedLocations',
                                                       'name')}

        # Loop through vi_features
        for k in video_features.keys():
            for feature_list in video_features[k]:
                for f in feature_list:
                    temp = dict()
                    temp = {
                        'vi_file_name': files,
                        'vi_source_language': video_insights['sourceLanguage'],
                        'vi_feature_type': k,
                        'vi_feature': f,
                        'vi_confidence_score': feature_list[f]
                    }
                    merged_insights.append(temp)
                    del temp

    return merged_insights


def putTableEntity(data):
    '''
    '''
    try:
        # Get Azure Storage Table connection
        logging.info('Creating Azure Storage Table')

        table_service = TableService(
            connection_string=os.environ['SA_CONNX_STRING'])

        try:
            logging.info('Checking for existing Azure Storage Table')

            # Check for table, if none then create new table
            table_service.create_table(os.environ['SA_TABLE_INSIGHTS'],
                                       fail_on_exist=True)

            logging.info('Success: New table created')
        except Exception as e:
            logging.info('Failed: Table already exists {0}'.format(e))

        for entity in data:
            try:
                # Create unique row key
                row_key = '{0}_{1}_{2}'.format(entity['vi_file_name'],
                                               entity['vi_feature_type'],
                                               entity['vi_feature'])

                task = {'PartitionKey': 'examplekey',
                        'RowKey': row_key,
                        'FileName': entity['vi_file_name'],
                        'SourceLanguage': entity['vi_source_language'],
                        'FeatureType': entity['vi_feature_type'],
                        'Feature': entity['vi_feature'],
                        'ConfidenceScore': entity['vi_confidence_score']}

                # Write entry to Azure Storage Table
                table_service.insert_or_merge_entity(
                    os.environ['SA_TABLE_INSIGHTS'], task)
            except Exception as e:
                logging.info(
                    'Failed: Put entity to Azure Storage Table {0}'.format(e))

        logging.info('Success: Put entity to Azure Storage Table')
    except Exception as e:
        logging.info(
            'Failed: Put entities to Azure Storage Table {0}'.format(e))


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting...')

    # Download Azure Storage Insights blobs to local files
    local_file_paths = getInsightsBlobsLocal()
    logging.info(local_file_paths)

    # Transform JSON data
    data = mergeInsights(local_file_paths)

    # Apply confidence cutoff to Video Indexer features
    confidence_cutoff = 0.0  # CHANGE TO CONTROL VI CONFIDENCE CUTOFF, 0.0 <= x <= 1.0
    data = [item for item in data if item['vi_confidence_score'] > confidence_cutoff]

    # Write features to Azure Storage Insights Table
    putTableEntity(data)

    # Delete local files
    [os.remove(f) for f in local_file_paths]

    logging.info('Completed.')

    return func.HttpResponse(
        'Success: Processed Video Indexer Insights stored results in Azure Storage Table',
        status_code=200)
