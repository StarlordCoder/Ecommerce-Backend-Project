import httpx,base64,random,json,time,hashlib
from py3rijndael import RijndaelCbc, ZeroPadding
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from protopy import userpayload_pb2,products_pb2
import json
from google.protobuf.json_format import Parse,MessageToJson,ParseDict

# import asyncio
# from motor.motor_asyncio import AsyncIOMotorClient

# async def show_users():
#     client = AsyncIOMotorClient("mongodb://localhost:27017")
#     db = client["fastapi_auth"]
#     users = db["products"]
#     async for doc in users.find():
#         print(doc)

# asyncio.run(show_users())

COMMON_KEY = "A$R1P12H7N" +"A$R1P12H7N"[::-1]+"A$R1P12H7N"+"J2"
COMMON_KEY = COMMON_KEY.encode()
print(COMMON_KEY)
COMMON_IV = b"0000000000000000"
print(COMMON_IV)

def generate_random_text(length=10):
    char_set = r"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ123456790"
    random_text = ''.join(random.choices(char_set, k=length))
    return random_text

def genprod(length=10):
    char_set = r"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ123456790"
    random_text = ''.join(random.choices(char_set, k=length))
    return random_text

def sha256_hash_string(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    return sha256_hash.digest()

def hash_string(input_string):
    sha256_hash = hashlib.md5()
    sha256_hash.update(input_string.encode('utf-8'))
    return sha256_hash.digest()

def encrypt(key,iv,plaintext):
    # Generate a random 16-byte IV
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # Pad plaintext to block size and encrypt
    ct_bytes = cipher.encrypt(pad(plaintext.encode('utf-8'), AES.block_size))
    # Combine IV + ciphertext for transmission
    encrypted = base64.b64encode(ct_bytes).decode('utf-8')
    return encrypted.replace("+","-").replace("/","_")

def decrypt(key,iv,encrypted):
    # Decode from base64
    encrypted_bytes = encrypted
    # Extract IV and ciphertext
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # Decrypt and unpad
    pt = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
    return pt.decode('utf-8')


def login():
    url = "http://127.0.0.1:8000/login"
    headers = {
        "Content-Type" : "application/x-protobuf"
    }
    data = {"email" : "test4@gmail.com","password":"pass@123"}
    json_data = {
        "message" :  "LOGIN",
        "payload" : encrypt(bytes(COMMON_KEY),bytes(COMMON_IV),json.dumps(data))
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    print(userpl.SerializeToString())
    r = httpx.post(url,data=userpl.SerializeToString())
    print(r.content)
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:32].replace("-","+").replace("_","/")
    iv = data[-16:].replace("-","+").replace("_","/")
    print(key)
    print(iv)
    data = decrypt(sha256_hash_string(key),hash_string(iv),base64.b64decode(data[32:-16].replace("-","+").replace("_","/")))
    print(data)
    return json.loads(data)["access_token"]
def register():
    url = "http://127.0.0.1:8000/register"
    headers = {
        "Content-Type" : "application/json"
    }
    data = {'email': 'Test5567676@gmail.com', 'phone': 9999999999, 'username': 'Dudehappen', 'password': 'Demo@123'}
    json_data = {
        "message" :  "REGISTER",
        "payload" : encrypt(COMMON_KEY,COMMON_IV,json.dumps(data))
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,data=userpl.SerializeToString())
    print(r.content)
    userpl.ParseFromString(r.content)
    data = base64.b64decode(userpl.payload).decode()
    data = decrypt(COMMON_KEY,COMMON_IV,base64.b64decode(data[32:-16].replace("-","+").replace("_","/")))
    print(data)

# register()

def pictures():
    url = "http://127.0.0.1:8000/products"
    headers = {
        "Authorization" : "Bearer "+token
    }
    r = httpx.get(url,headers=headers)
    print(r.content)
    file = open("protobin.bin","wb")
    file.write(r.content)
    userpl = userpayload_pb2.Payload()
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    print(key)
    print(iv)
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)
    # products = products_pb2.PicPayload()
    # products.ParseFromString(r.content)
    # print(products.assets)
    # uid = "sboVAtf7GTgq"
    # data = base64.b64decode(r.json()["payload"]).decode()
    # data = decrypt(sha256_hash_string(data[:20]+uid),hash_string(data[-4:]+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    # print(json.loads(data))

def bags():
    url = "http://127.0.0.1:8000/bags"
    headers = {
        "Content-Type" : "application/x-protobuf",
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MTgyMzA0MSIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MTkwOTQ0MX0.uZxI0zrV3piAuTKl2Jblxrb5QTw6OwohxkZLKm3OWjM"
    }
    uid = "sboVAtf7GTgq"
    data = {"name" : "Nike","price":"₹2999.00","image":"https://plus.unsplash.com/premium_photo-1683140435505-afb6f1738d11?fm=jpg&q=60&w=3000&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8MXx8c2hpcnR8ZW58MHx8MHx8fDA%3D","userid":uid}
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "addproducts",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,headers=headers,data=userpl.SerializeToString())
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

def cart():
    url = "http://127.0.0.1:8000/bag/add"
    headers = {
        "Content-Type" : "application/x-protobuf",
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MTgyMzA0MSIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MTkwOTQ0MX0.uZxI0zrV3piAuTKl2Jblxrb5QTw6OwohxkZLKm3OWjM"
    }
    uid = "sboVAtf7GTgq"
    data =  {
      "product_id": "bag_002",
      "name": "Leather Backpack",
      "price_at_addition": 79.99,
      "quantity": 2,
      "discount": 5.0
    }
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "addcart",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,headers=headers,data=userpl.SerializeToString())
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

def cartup():
    url = "http://127.0.0.1:8000/bag/itemup"
    headers = {
        "Content-Type" : "application/x-protobuf",
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MTgyMzA0MSIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MTkwOTQ0MX0.uZxI0zrV3piAuTKl2Jblxrb5QTw6OwohxkZLKm3OWjM"
    }
    uid = "sboVAtf7GTgq"
    data =  {
      "product_id": "bag_001",
      "name": "Leather Backpack",
      "price_at_addition": 79.99,
      "quantity": 3,
      "discount": 5.0
    }
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "addcart",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,headers=headers,data=userpl.SerializeToString())
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

def cartdown():
    url = "http://127.0.0.1:8000/bag/itemdown"
    headers = {
        "Content-Type" : "application/x-protobuf",
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MTgyMzA0MSIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MTkwOTQ0MX0.uZxI0zrV3piAuTKl2Jblxrb5QTw6OwohxkZLKm3OWjM"
    }
    uid = "sboVAtf7GTgq"
    data =  {
      "product_id": "bag_002",
      "name": "Leather Backpack",
      "price_at_addition": 79.99,
      "quantity": 3,
      "discount": 5.0
    }
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "addcart",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,headers=headers,data=userpl.SerializeToString())
    print(r.status_code)
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

def nbags():
    url = "http://127.0.0.1:8000/mybags"
    headers = {
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MTkxNjEyMyIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MjAwMjUyM30.a9FV8f_VdPvSFHgLYJo53qqODB9IEJU6aC3KNSXeOa0"
    }
    userpl = userpayload_pb2.Payload()
    r = httpx.get(url,headers=headers)
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload).decode()
    data = decrypt(sha256_hash_string(data[:20]+uid),hash_string(data[-4:]+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(json.loads(data))

def myorders():
    url = "http://127.0.0.1:8000/myorders"
    headers = {
        "Authorization" : "Bearer "+token
    }
    userpl = userpayload_pb2.Payload()
    r = httpx.get(url,headers=headers)
    print(r.content)
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload).decode()
    data = decrypt(sha256_hash_string(data[:20]+uid),hash_string(data[-4:]+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(json.loads(data))

def checkout():
    url = "http://127.0.0.1:8000/bag/checkout"
    headers = {
        "Authorization" : "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0NEBnbWFpbC5jb20iLCJyb2xlIjoidXNlciIsInRzIjoiMTc1MjIyMDAwNSIsInVpZCI6InNib1ZBdGY3R1RncSIsImV4cCI6MTc1MjMwNjQwNX0.q9EWBnhkCU5u_3N97iDW8RfxrJ7cBn2RlVNK9fFPTo4"
    }
    r = httpx.get(url,headers=headers)
    userpl = userpayload_pb2.Payload()
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

def createorder():
    url = "http://127.0.0.1:8000/create_order"
    headers = {
        "Authorization" : "Bearer "+token
    }
    data = {
  "userid": "sboVAtf7GTgq",
  "items": [
    {
      "name": "Puma",
      "price": 2999,
      "product_id": "proNhfxYnodQh",
      "size" : "L",
      "quantity": 2,
      "image": "https://plus.unsplash.com/premium_photo-1683140435505-afb6f1738d11?fm=jpg&q=60&w=3000&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8MXx8c2hpcnR8ZW58MHx8MHx8fDA%3D"
    }
  ],
  "totals": {
    "subtotal": 5998,
    "discount": 719,
    "tax_amount": 720,
    "shipping_cost": 150,
    "total": 6149
  }
}
    uid = "sboVAtf7GTgq"
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "createorder",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    r = httpx.post(url,headers=headers,data=userpl.SerializeToString())
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)


token = login()
myorders()

def orderup():
    url = "http://localhost:8000/orderstatus/update"
    data = {'order_id': '47578e6a-10d7-4ebf-a26f-f8d5dcc1159c', 'status': 'Delivered',"partner" : "ABC123"}
    uid = "sboVAtf7GTgq"
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc= key+encrypt(sha256_hash_string(key+uid),hash_string(iv+uid),json.dumps(data))+iv
    json_data = {
        "message" :  "orderupdate",
        "payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    print("Len : "+str(len(json.dumps(json_data))))
    userpl = userpayload_pb2.Payload()
    ParseDict(json_data,userpl)
    print("Prolen : "+str(len(userpl.SerializeToString())))
    # data = '{"customer_id":"cust002","items":[{"item_id":"burger","quantity":2}]}'
    r = httpx.post(url,headers={"Authorization" : "Bearer "+token},data=userpl.SerializeToString())
    userpl.ParseFromString(r.content)
    uid = "sboVAtf7GTgq"
    data = base64.b64decode(userpl.payload.replace("-","+").replace("_","/")).decode()
    key = data[:20].replace("-","+").replace("_","/")
    iv = data[-4:].replace("-","+").replace("_","/")
    data = decrypt(sha256_hash_string(key+uid),hash_string(iv+uid),base64.b64decode(data[20:-4].replace("-","+").replace("_","/")))
    print(data)

# # orderup()
# login()

# url = "http://localhost:8000/orderstatus/update"
# data = {'order_id': '47578e6a-10d7-4ebf-a26f-f8d5dcc1159c', 'status': 'Delivered',"partner" : "ABC123"}
# # data = '{"customer_id":"cust002","items":[{"item_id":"burger","quantity":2}]}'
# r = httpx.post(url,json=data)
# print(r.text)


# url = "http://localhost:8000/orderstatus/update"
# data = {'order_id': '7b2026cb-154b-4dab-894c-928883fb0ae4', 'status': 'Delivered',"partner" : "ABC123"}
# # data = '{"customer_id":"cust002","items":[{"item_id":"burger","quantity":2}]}'
# r = httpx.post(url,json=data)
# print(r.text)


# login()
# from motor.motor_asyncio import AsyncIOMotorClient
# from pymongo import MongoClient

# MONGO_URI = "mongodb://localhost:27017"
# client = AsyncIOMotorClient(MONGO_URI)
# db = client["fastapi_auth"]
# user_collection = db["users"]
# prod_collection = db["products"]
# bags_collection = db["custbags"]
# cart_collection = db["usercart"]
# orders = db["orders"]

# from motor.motor_asyncio import AsyncIOMotorClient
# from datetime import datetime

# async def get_clean_items():
#     client = AsyncIOMotorClient("mongodb://localhost:27017")
#     db = client["fastapi_auth"]
#     collection = db["orders"]

#     cursor = collection.find({"user_id": "sboVAtf7GTgq"})  # Your filter here
#     clean_items = []

#     async for doc in cursor:
#         cleaned_doc = {
#             k: v for k, v in doc.items()
#             if k != "_id" and not isinstance(v, datetime)
#         }
#         clean_items.append(cleaned_doc)

#     return clean_items
# import asyncio
# async def main():
#     items = await get_clean_items()
#     print(items)

# asyncio.run(main())