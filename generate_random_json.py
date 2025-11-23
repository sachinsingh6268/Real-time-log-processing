import uuid
import random
import time
import json


def random_user_log():
  request_id = str(uuid.uuid4())
  user_id = random.randint(1,1_000_000)
  session_id = str(uuid.uuid4())
  ip_address = ".".join([str(random.randint(0,255)) for _ in range(4)])
  url = random.choice(["product","items"]) + "/" + str(random.randint(1,10_000))
  method = random.choice(["GET","POST","PUT","PATCH","DELETE"])
  status_code = random.choice([101,102,200,201,204,301,302,400,401,403,404,429,500,502,503,504])
  response_time_ms = random.randint(10,1000)
  bytes_sent = random.randint(500,100_000_000)
  browser = random.choice(["Chrome","Safari","Edge","Firefox","Brave","Opera"])
  os = random.choice(["Windows", "macOS", "Linux", "Android", "ChromeOS", "iOS"])
  device_type = random.choice(["Mobile","Laptop","Tablet"])
  country = "India"
  state = random.choice(["Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat","Haryana","Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh","Uttarakhand","West Bengal"])
  traffic_source = random.choice(["google_ads","facebook_ads","instagram","email_campaign","direct","organic_search","referral","linkedin","twitter","push_notification","affiliate","youtube_ads","bing_ads"])
  is_authenticated = random.choice([True,False])
  timestamp = time.strftime("%Y-%m-%d")

  user_json = {
    "request_id": request_id,
    "user_id": user_id,
    "session_id": session_id,
    "ip_address": ip_address,
    "url": url,
    "method": method,
    "status_code": status_code,
    "response_time_ms": response_time_ms,
    "bytes_sent": bytes_sent,
    "user_agent": {
      "browser": browser,
      "os": os,
      "device_type": device_type
    },
    "geo": {
      "country": country,
      "state": state
    },
    "traffic_source": traffic_source,
    "is_authenticated": is_authenticated,
    "ts": timestamp
  }

  return user_json

def create_user_json_list(n:int):
  user_log_list = []
  # Creating a list of json objects
  for _ in range(n):
    user_log_list.append(random_user_log())

  return user_log_list


def dump_json_to_file(folder_path: str):
  no_of_json_objects = random.randint(10,600)
  json_objects = create_user_json_list(no_of_json_objects)

  # dumping the json objects to file
  file_name = folder_path + "/user_logs_" + str(time.time_ns()) + ".json"
  with open(file_name,"w") as f:
    for obj in json_objects:
      f.write(json.dumps(obj) + "\n")

if __name__ == "__main__":
  folder = "/home/sachin/Downloads/Datasets/Input/Real_time_logs"
  while True:
    dump_json_to_file(folder)
    time.sleep(2)


