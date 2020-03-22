# -*- coding: utf-8 -*-

# related to google api 
from __future__ import print_function
import pickle
import os.path
import os
import io
import sys
import json
import requests
import boto3
import logging
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from PIL import Image  
from botocore.exceptions import ClientError


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = ['1NMjHYDZc5s_EixE67UJA2gxSd0DQD-oQOkiYg6Gylz0','1QDwtP65qOb0owkzryC5QysGunG_-O7SjPcA1gwAJYWk']
URL = ['http://54.180.127.176:8000/jeokSyeo/createBrewery','http://54.180.127.176:8000/jeokSyeo/createAlchol']
BASE_IMG_URL = 'https://jeoksyeo.s3.ap-northeast-2.amazonaws.com'
BREWERY = 0
ALCHOL = 1
WHICH = 0

sheet_name = 'sheet1'
start_cell = 'U1'
start_row = 0
finish_cell = 'U2'
finish_row = 0

def deleteImg(img_name):
  os.remove(img_name)
  # print('dlete')

def requestToServer(obj):
  response = requests.post(URL[WHICH], data = obj)
  res_json = json.loads(response.text)
  
  # print(res_json)
  return res_json

def uploadToS3(img_path,img_name):
  Log('upload image')
  client = boto3.client('s3',region_name='ap-northeast-2')
  try:
    response = client.upload_file(img_name,'jeoksyeo', img_path + img_name)  
  except ClientError as e:
    logging.error(e)
    return False

  return True

def checkImageExist(img_path,img_name):
  Log('check Image Exist')
  status = False
  client = boto3.client('s3',region_name='ap-northeast-2')
  try:
    client.list_objects(Bucket='jeoksyeo', Prefix=img_path + img_name)['Contents']
  except KeyError :
    status = True
  
  return status

def downloadImage(drive,img_name,img_url):
  image_id = img_url.split("?id=")[1]
  name = img_name + '.jpg'
  request = drive.get_media(fileId=image_id)
  fh = io.FileIO(name,'wb')
  downloader = MediaIoBaseDownload(fh,request)
  done = False

  while done is False:
    status, done = downloader.next_chunk()

  # 리사이즈 관련
  # size = 500
  # origin_img = Image.open(name)
  # origin_img.thumbnail(size, Image.ANTIALIAS)
  # origin_img.save("tmp.jpg","JPEG")    
  return name

def writeMessage(sheet,status, msg, idx):
  if WHICH == ALCHOL :
    status_cell = 'R'
    msg_cell = 'S'
  else :
    status_cell = 'M'
    msg_cell = 'N'
  
  range = sheet_name + "!" + status_cell + str(idx) + ':' + msg_cell + str(idx)

  values = [
    [
      status,
      msg
    ],
  ]
  body = {
    'values' : values
  }

  sheet.values().update(spreadsheetId = SPREADSHEET_ID[WHICH],
                        range = range,
                        valueInputOption='USER_ENTERED',
                        body = body).execute()

def check_already_update(sheet,idx):
  if WHICH == ALCHOL :
    status_cell = 'R'
  else :
    status_cell = 'M'
  
  range = sheet_name + "!" + status_cell + str(idx)

  try:
    status = sheet.values().get(spreadsheetId=SPREADSHEET_ID[WHICH],
                                range=range).execute().get('values',[])[0][0]
  except IndexError :
    return False

  if status == 'O':
    return True
  
  return False


def transferToDBBrewery(sheet,drive,values):
  idx = 0
  
  for row in values:
    cell_idx = int(start_row) + idx
    
    # 성공 유무가 O인지 체크
    if check_already_update(sheet,cell_idx):
      while True:
        flag = raw_input(str(idx) + ' is already checked O if you want to overwrite?[y/n] : ')

        if flag == 'y' or flag == 'n':
          break;
      if flag == 'n' :
        idx += 1
        continue

    try :
      email = row[1]
      phone_number = row[2]
      name_kor = row[3]
      name_eng = row[4]
      description = row[6]
      location_si = row[7]
      location_gu = row[8]
      location = location_si + " " + location_gu
      img_url = row[11]
      homepage_url = row[10]
    except IndexError :
      status = 'X'
      msg = "Img url is miss"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    if img_url is None : 
      status = 'X'
      msg = "Img url is miss"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    name_eng = name_eng.title()
    img_name = name_eng.replace(" ","_")

    img_name = downloadImage(drive,img_name,img_url)

    if checkImageExist('brewery/',img_name) == False:
      deleteImg(img_name)
      status = 'X'
      msg = "Maybe Img File already exist"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    img_url = BASE_IMG_URL + '/brewery/' + img_name

    obj = {
      'name_kor' : name_kor,
      'name_eng' : name_eng,
      'img_url' : img_url,
      'location' : location,
      'email' : email,
      'phone_number' : phone_number,
      'description' : description,
      'homepage_url' : homepage_url
      }

    response = requestToServer(obj)
    # Commit message
    if response['status'] == True and uploadToS3('brewery/',img_name) == True:
      status = 'O'
      msg = "Success"
      writeMessage(sheet,status,msg, cell_idx)
    else :
      status = 'X'
      msg = response['msg']
      writeMessage(sheet,status,msg, cell_idx)

    # delete image in local
    deleteImg(img_name)

    idx += 1


# 위의 함수랑 똑같이 출력하면 됌 
def transferToDBAlchol(sheet,drive,values):
  idx = 0

  for row in values:
    cell_idx = int(start_row) + idx

    if check_already_update(sheet,cell_idx):
      while True:
        flag = raw_input(str(idx) + ' is already checked O if you want to overwrite?[y/n] : ')

        if flag == 'y' or flag == 'n':
          break;
      if flag == 'n' :
        idx += 1
        continue

    try :
      name_kor = row[1]
      name_eng = row[2]
      type_kor = row[3]
      degree = row[4]
      description = row[5]
      ypos = row[6]
      xpos = row[7]
      main_img_url = row[15]
      remove_background_img_url = row[16]
      food_paring = row[10]
      brewery_name = row[14]
    except IndexError:
      status = 'X'
      msg = "Main img url is miss"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    if main_img_url is None or \
       remove_background_img_url is None: 
      status = 'X'
      msg = "Img url is miss"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    if brewery_name == '' or brewery_name is None:
      status = 'X'
      msg = "Brewery name is miss"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    name_eng = name_eng.title()
    img_name = name_eng.replace(" ","_")
    remove_background_img_name = img_name + "_remove"

    main_img_name = downloadImage(drive,img_name,main_img_url)
    remove_background_img_name = downloadImage(drive,remove_background_img_name,remove_background_img_url)


    if checkImageExist('alchol/',main_img_name) == False:
      deleteImg(main_img_name)
      status = 'X'
      msg = "Maybe Main Img File already exist"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue
      
    if checkImageExist('alchol/',remove_background_img_name) == False:
      deleteImg(remove_background_img_name)
      status = 'X'
      msg = "Maybe Remove Background Img File already exist"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    main_img_url = BASE_IMG_URL + '/alchol/' + main_img_name
    remove_background_img_url = BASE_IMG_URL + '/alchol/' + remove_background_img_name
    

    if type_kor == "탁주".decode('utf-8'):
      type_eng = 'TA'
    elif type_kor == "약주".decode('utf-8'):
      type_eng = 'YA'
    elif type_kor == "청주".decode('utf-8'):
      type_eng = 'CH'
    elif type_kor == "맥주".decode('utf-8'):
      type_eng = 'BE'
    elif type_kor == "과실주".decode('utf-8'):
      type_eng = 'FR'
    elif type_kor == "소주 · 증류주".decode('utf-8'):
      type_eng = 'DI'
    elif type_kor == "위스키".decode('utf-8'):
      type_eng = 'WH'
    elif type_kor == "리큐르".decode('utf-8'):
      type_eng = 'LI'
    elif type_kor == "무알코올".decode('utf-8'):
      type_eng = 'NO'
    else:
      deleteImg(remove_background_img_name)
      status = 'X'
      msg = "Invalid alchol Type"
      writeMessage(sheet,status,msg, cell_idx)
      idx += 1
      continue

    
    obj = {
      'name_kor' : name_kor,
      'name_eng' : name_eng,
      'type' : type_eng,
      'description' : description,
      'main_img_url' : main_img_url,
      'remove_background_img_url' : remove_background_img_url,
      'degree' : degree,
      'xpos' : xpos,
      'ypos' : ypos,
      'brewery_name' : brewery_name,
    }
    response = requestToServer(obj)
    if response['status'] == True and \
       uploadToS3('alchol/',main_img_name) == True and \
       uploadToS3('alchol/',remove_background_img_name) == True:
      status = 'O'
      msg = "Success"
      writeMessage(sheet,status,msg, cell_idx)
    else :
      status = 'X'
      msg = response['msg']
      writeMessage(sheet,status,msg, cell_idx)


    deleteImg(main_img_name)
    deleteImg(remove_background_img_name)

    idx += 1


def transferToDB(sheet,drive,values):
  if WHICH == ALCHOL:
    transferToDBAlchol(sheet,drive,values)
  else:
    transferToDBBrewery(sheet,drive,values)
  
def getResponse(sheet,range):
  request = sheet.values().get(spreadsheetId=SPREADSHEET_ID[WHICH],
                              range=range)
  
  response = request.execute()

  return response

def getRange(sheet):
  global start_row
  global finish_row

  start_range = sheet_name + '!' + start_cell

  try :
    start_row = sheet.values().get(spreadsheetId=SPREADSHEET_ID[WHICH],
                                    range=start_range).execute().get('values',[])[0][0]
  except IndexError :
    Error("enter start row")

  finish_range = sheet_name + '!' + finish_cell

  try : 
    finish_row = sheet.values().get(spreadsheetId=SPREADSHEET_ID[WHICH],
                                    range=finish_range).execute().get('values',[])[0][0]
  except IndexError :
    Error("enter finish row")

  if start_row > finish_row :
    Error("Start row and finish row error")

  if WHICH == BREWERY:
    range = sheet_name + '!' + 'A' + start_row + ':' + 'L' + finish_row
  elif WHICH == ALCHOL:
    range = sheet_name + '!' + 'A' + start_row + ':' +'Q' + finish_row

  return range

def getAccess():
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

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    return sheet

def getAccessDrive():
  creds = None
  # The file token.pickle stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists('token_drive.pickle'):
      with open('token_drive.pickle', 'rb') as token:
          creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
          creds.refresh(Request())
      else:
          flow = InstalledAppFlow.from_client_secrets_file(
                'credentials_drive.json', DRIVE_SCOPES)
          creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
      with open('token_drive.pickle', 'wb') as token:
          pickle.dump(creds, token)

  service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API
  drive = service.files()
  return drive


def print_help():
  print('Usage : python syncWithDB.py brewery')
  print('\tpython syncWithDB.py alchol')

def Log(msg):
  print("[DEBUG] " + msg)

def Error(msg):
  print("[ERROR] " + msg)
  sys.exit()

def main():
    global WHICH

    if len(sys.argv) != 2:
      print_help()
      return 0
    elif sys.argv[1] != 'brewery' and sys.argv[1] != 'alchol':
      print_help()
      return 0    

    if sys.argv[1] == 'brewery':
      Log("you choose brewery")
      WHICH = BREWERY
    elif sys.argv[1] == 'alchol':
      Log("you choose alchol")
      WHICH = ALCHOL
  
    sheet = getAccess()
    drive = getAccessDrive()
    Log("success to get access")
    
    range = getRange(sheet)
    Log("range : " + range)

    response = getResponse(sheet,range)
    Log("reponse success")

    values = response.get('values',[])
    
    if not values:
      Log('No data found')
      return 0

    # get info 
    transferToDB(sheet,drive,values)

    # # update start_row
    # updateStartRow(sheet)

if __name__ == '__main__':
    main()