from __future__ import print_function
import pickle
import os.path
import zipfile
import io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from googleapiclient.http import MediaIoBaseDownload

import random

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API
    results = service.files().list(
        pageSize=1000, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        
        my_files = {}
        for item in items:
            if item['name'].startswith('liveNoGreeks'):
                my_files[item['name']] =  item['id']
        
        print("\n")
        
        get_year = lambda x: int(x[len('liveNoGreeks'):len('liveNoGreeks')+4])
        my_files_sorted = sorted(my_files, key=get_year)

        # Check that all are present
        min_year = get_year(my_files_sorted[0])
        max_year = get_year(my_files_sorted[len(my_files)-1])
        for y in range(min_year, max_year+1):
            filename = 'liveNoGreeks' + str(y)
            assert(filename in my_files);
            
            results = service.files().list(
                pageSize=1000, 
                fields="nextPageToken, files(id, name)", 
                q="'{id}' in parents".format(id=my_files[filename])).execute()
            child_items = results.get('files', [])
            for child in child_items:
                print('In ' + filename + ', File Name ' + child['name'])

                request = service.files().get_media(fileId=child['id'])
                with open(child['name'], "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        print("\tDownload ", int(status.progress() * 100))
                with zipfile.ZipFile(child['name'], 'r') as zip_ref:
                    zip_ref.extractall(".")
    
if __name__ == '__main__':
    main()
