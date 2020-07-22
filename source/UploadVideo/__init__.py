import os
import logging
import requests
from pathlib import Path
import azure.functions as func


def putVideo(blob_path, blob_name, blob_uri):
    '''
    '''
    try:
        # Attempt connection to VI for Access Token
        logging.info('Sending blob information to PutVideo function')

        # Format HTTPS Request
        params = {'path': blob_path,
                  'name': blob_name,
                  'uri': blob_uri}
        request_url = os.environ['AF_PUTVIDEO_URL']

        # Get Video Indexer access token
        response = requests.get(request_url,
                                params=params)
        response.raise_for_status()

        logging.info('Success: Sent blob information to PutVideo function')

        # Return Video Indexer access token
        return response.json()
    except Exception as e:
        logging.info(
            'Failed: Send blob information to PutVideo Function: {0}'.format(e))


def main(myblob: func.InputStream):
    logging.info('Starting...')

    # Get blob params
    sa_blob_path = str(myblob.name)
    sa_blob_name = str(Path(sa_blob_path).stem)
    sa_blob_uri = str(myblob.uri)

    supported_formats = ['.mxf', '.gxf', '.ts', '.ps', '.3gp',
                         '.3gpp', '.mpg', '.wmv', '.asf', '.avi',
                         '.mp4', '.m4a', '.m4v', '.isma', '.ismv',
                         '.dvr-ms', '.mkv', '.wav', '.mov']

    # Check for supported video format in VI
    if Path(sa_blob_path).suffix in supported_formats:
        # Get Video Indexer access token and upload video
        putVideo(sa_blob_path, sa_blob_name, sa_blob_uri)
    else:
        logging.info(
            '{0} unsupported file format in Video Indexer'.format(sa_blob_path))

    logging.info('Completed.')
