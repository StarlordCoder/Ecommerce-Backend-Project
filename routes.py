from fastapi import APIRouter, HTTPException, status, Depends,Request,Response
from database import user_collection,prod_collection,bags_collection,cart_collection,pay_collection,orders,delivery
from models import UserRegister, UserLogin, Token,ProdData
from fastapi.encoders import jsonable_encoder
from fastapi.security import OAuth2PasswordBearer
from auth import hash_password, verify_password, create_access_token,generate_random_text,encrypt,decrypt,COMMON_KEY,COMMON_IV,decode_access_token,serialize_doc,sha256_hash_string,hash_string
from pymongo.errors import DuplicateKeyError
import json,time,base64
import uuid
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer
import asyncio
import threading
from datetime import datetime
# from confluent_kafka import Producer, Consumer
from protopy import userpayload_pb2,products_pb2
import json
from google.protobuf.json_format import Parse,MessageToJson,ParseDict

router = APIRouter()



@router.post("/register", status_code=200)
async def register(user: Request):
    raw_data = await user.body()
    print(raw_data)
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    dec_data = json.loads(decrypt(COMMON_KEY.encode(),COMMON_IV.encode(),person.payload))
    print(dec_data)
    key = COMMON_KEY
    iv = COMMON_IV
    if await user_collection.find_one({"email": dec_data["email"]}):
        data = {"message": "Email already registered"}
        enc = key+encrypt(key.encode(),iv.encode(),json.dumps(data))+iv
        data = {"message" : "error","payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
    
    guserid = "sb"+generate_random_text()
    user_dict = {
        "userid" : guserid,
        "email": dec_data["email"],
        "phone" : dec_data["phone"],
        "username": dec_data["username"],
        "hashed_password": hash_password(dec_data["password"]),
        "role": "user",
        "is_verified": False
    }
    try:
        await user_collection.insert_one(user_dict)
    except DuplicateKeyError:
        data = {"message": "User already exists"}
        enc = key+encrypt(key.encode(),iv.encode(),json.dumps(data))+iv
        data = {"message" : "error","payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
    
    data = {"message": "Success","Userid" : guserid}
    print(data)
    enc = key+encrypt(key.encode(),iv.encode(),json.dumps(data))+iv
    print(enc)
    data = {"message" : "Registered Succesfully","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    # return {"message": "Success", "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/login", status_code=200)
async def login(user: Request):
    raw_data = await user.body()
    print(len(raw_data))
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    print(person.payload)
    dec_data = json.loads(decrypt(COMMON_KEY.encode(),COMMON_IV.encode(),person.payload))
    db_user = await user_collection.find_one({"email": dec_data["email"]})
    if not db_user or not verify_password(dec_data["password"], 
                                          db_user["hashed_password"]):
        raise HTTPException(401, "Invalid credentials")

    token_data = {"sub": db_user["email"], "role": db_user["role"],"ts":str(int(time.time())),"uid" : db_user["userid"]}
    token = create_access_token(token_data)
    data = {"access_token": token, "token_type": "bearer","userid" : db_user["userid"]}
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc = key+db_user["userid"]+encrypt(sha256_hash_string(key+db_user["userid"]),hash_string(iv+db_user["userid"]),json.dumps(data))+iv+db_user["userid"]
    data = {"message": "Success", "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    # return {"message": "Success", "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# JWT verification + MongoDB user fetch
def verify_token(token: str):
    try:
        payload = decode_access_token(token)
        user_email = payload.get("sub")
        if user_email is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return user_email
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate token",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_current_user(token: str = Depends(oauth2_scheme)):
    print(token)
    email = verify_token(token)
    user = await user_collection.find_one({"email": email})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.get("/products")
async def submit_json(
    current_user: dict = Depends(get_current_user)
):
    cursor = prod_collection.find({})
    documents = []
    async for doc in cursor:
        try:
            if doc["price"] == "₹1999.00":
                documents.append({"name" : doc["name"],"image" : doc["image"],"price" : doc["price"],"sex":"Women","product_id":doc["productId"],"shop" : doc["store_name"]})
                documents.append({"name" : doc["name"],"image" : doc["image"],"price" : doc["price"],"sex":"Women","product_id":doc["productId"],"shop" : doc["store_name"]})
            else:
                documents.append({"name" : doc["name"],"image" : doc["image"],"price" : doc["price"],"sex":"Men","product_id":doc["productId"],"shop" : doc["store_name"]})
        except:
                documents.append({"name" : doc["title"],"image" : doc["image"],"price" : doc["price"],"sex":"Men","product_id":doc["productId"],"shop" : doc["store_name"]})
    data = {"assets" : {"pimages":documents,"cover" : ["https://www.beigebrown.com/cdn/shop/articles/BB-blog_spring_summer_men-cover_9adb79ff-24c5-4db5-80ad-2bf0a7f25d96.jpg?v=1743697509&width=2048",
  "https://boldoutline.in/wp-content/uploads/2024/02/Web-cover-10.jpg",
  "https://lh3.googleusercontent.com/wujK76micLULk7o7RBBRjkB6aoupvGjTZRTVNWmp6J6uCoSj4mtOJCbZvGwD5OIofMlhjcHrgu1eHhQnQwnjEEYsn1cQ=h450-rw",
  "https://cdn.pixabay.com/photo/2017/03/27/13/28/man-2178721_1280.jpg",
  "https://img-cdn.thepublive.com/filters:format(webp)/elle-india/media/post_attachments/wp-content/uploads/2023/10/4e859dfe-7dbc-4e37-8b1c-0a1a9cf0aec9.jpeg"
]}}
    # products = products_pb2.PicPayload()
    # ParseDict(data,products)
    # print(products.SerializeToString())
    # # return {"message": "Success", "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    # return Response(content=products.SerializeToString(), media_type="application/x-protobuf")
    person = userpayload_pb2.Payload()
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {
        "message": "Success",
        "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

@router.get("/userinfo")
async def submit_json(
    current_user: dict = Depends(get_current_user)
):
    data = {"data" : {"username" : current_user["username"],"email" : current_user["email"],"verified" : current_user["is_verified"],"phone" : current_user["phone"]} }
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    person = userpayload_pb2.Payload()
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data= {
        "message": "Success",
        "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    ParseDict(data,person)
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/bags")
async def bags(user: Request,current_user: dict = Depends(get_current_user)):
    raw_data = await user.body()
    print(len(raw_data))
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    print(person.payload)
    data = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    dec_data = json.loads(decrypt(sha256_hash_string(data[:20]+current_user["userid"]),hash_string(data[-4:]+current_user["userid"]),data[20:-4]))
    user_dict = {"name" : dec_data["name"],"image" : dec_data["image"],"price" : dec_data["price"],"userid":dec_data["userid"]}
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    try:
        await bags_collection.insert_one(user_dict)
    except DuplicateKeyError:
        data = {"message": "User already exists"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        raise HTTPException(200, {"payload" : base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")})
    data = {"status": "Product Added Successfully"}
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")


@router.post("/bag/add")
async def add_item_to_cart(user: Request,current_user: dict = Depends(get_current_user)):
    cart = await cart_collection.find_one({"userid": current_user["userid"]})
    raw_data = await user.body()
    print(raw_data)
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    print(person.payload)
    data = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    dec_data = json.loads(decrypt(sha256_hash_string(data[:20]+current_user["userid"]),hash_string(data[-4:]+current_user["userid"]),data[20:-4]))
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    if not cart:
        new_cart = {
            "userid": current_user["userid"],
            "items": [dec_data],
            "status": "active",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        result = await cart_collection.insert_one(new_cart)
        data = {"msg": "Cart created", "cart_id": str(result.inserted_id)}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

    for i, existing_item in enumerate(cart["items"]):
        if existing_item["product_id"] == dec_data["product_id"]:
            cart["items"][i]["quantity"] += dec_data["quantity"]
            break
    else:
        cart["items"].append(dec_data)

    cart["updated_at"] = datetime.utcnow()
    await cart_collection.update_one({"userid":current_user["userid"]}, {"$set": {"items": cart["items"], "updated_at": cart["updated_at"]}})
    data =  {"msg": "Item added"}
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/bag/itemup")
async def add_item_to_cart(user: Request,current_user: dict = Depends(get_current_user)):
    cart = await cart_collection.find_one({"userid": current_user["userid"]})
    raw_data = await user.body()
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    print(person.payload)
    data = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    dec_data = json.loads(decrypt(sha256_hash_string(data[:20]+current_user["userid"]),hash_string(data[-4:]+current_user["userid"]),data[20:-4]))
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    if not cart:
        data = {"msg": "Cart not found"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
    print(dec_data)
    for item in cart["items"]:
        for ditems in dec_data:
            if item["product_id"] == ditems["product_id"]:
                item["quantity"] = ditems["quantity"]
                break
        else:
            data = {"msg": "Item not found in cart"}
            enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
            data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
            ParseDict(data,person)
            print(person.SerializeToString())
            return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
            
    await cart_collection.update_one({"userid" : current_user["userid"]}, {"$set": {"items": cart["items"], "updated_at": datetime.utcnow()}})
    data = {"msg": "Item quantity updated"}
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/bag/itemdown")
async def add_item_to_cart(user: Request,current_user: dict = Depends(get_current_user)):
    cart = await cart_collection.find_one({"userid": current_user["userid"]})
    raw_data = await user.body()
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    print(person.payload)
    data = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    dec_data = json.loads(decrypt(sha256_hash_string(data[:20]+current_user["userid"]),hash_string(data[-4:]+current_user["userid"]),data[20:-4]))
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    if not cart:
        data = {"msg": "Cart not found"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(status_code=404,content=person.SerializeToString(), media_type="application/x-protobuf")
    updated_items = [item for item in cart["items"] if item["product_id"] != dec_data["product_id"]]
    if len(updated_items) == len(cart["items"]):
        data = {"msg": "Item not found in cart"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
        
    await cart_collection.update_one({"userid" : current_user["userid"]}, {"$set": {"items": updated_items, "updated_at": datetime.utcnow()}})
    data = {"msg": "Item removed from cart"}
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")

@router.get("/bag/checkout")
async def add_item_to_cart(current_user: dict = Depends(get_current_user)):
    cart = await cart_collection.find_one({"userid": current_user["userid"]})
    person = userpayload_pb2.Payload()
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    if not cart:
        data = {"msg": "Cart not found"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(status_code=404,content=person.SerializeToString(), media_type="application/x-protobuf")
    if not cart["items"]:
        data = {"msg": "Cart is Empty"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(content=person.SerializeToString(), media_type="application/x-protobuf")
        
    subtotal =sum(item["price"] * item["quantity"] for item in cart["items"])
    total_discount = int(sum(((item.get("price") / 100) * 12)  * item["quantity"] for item in cart["items"]))
    tax = int(round(subtotal * 0.12 if subtotal > 1000 else subtotal *0))  # 10% tax
    shipping = int(round(sum([x["price"]*0.05 for x in cart["items"] if x["price"] < 10000]))) #5.99 if subtotal < 100 else 0.0
    total = (subtotal - total_discount) + tax + shipping

    await cart_collection.update_one({"userid": current_user["userid"]}, {
        "$set": {
            "status": "checked_out",
            "totals": {
                "subtotal": subtotal,
                "discount": total_discount,
                "tax_amount": tax,
                "shipping_cost": shipping,
                "total": total
            },
            "updated_at": datetime.utcnow()
        }
    })
    data = {
        "msg": "Checkout complete",
        "totals": {
            "subtotal": subtotal,
            "discount": total_discount,
            "tax": tax,
            "shipping": shipping,
            "total": total
        }
    }
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/create_order")
async def createorder(user: Request,current_user: dict = Depends(get_current_user)
):  
    raw_data = await user.body()
    person = userpayload_pb2.Payload()
    person.ParseFromString(raw_data)
    order_id = str(uuid.uuid4())
    razor_id = str(uuid.uuid4())
    data1 = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    data = json.loads(decrypt(sha256_hash_string(data1[:20]+current_user["userid"]),hash_string(data1[-4:]+current_user["userid"]),data1[20:-4]))
    order_data = {
        "order_id": order_id,
        "razorpay_order_id": order_id,
        "user_id": data["userid"],
        "items": data["items"],
        "total_amount": data["totals"]["total"],
        "status": "pending",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    orders.insert_one(order_data)
    # Optionally emit order-created event
    # send_kafka("orders", order_data)
    
    payment_event = {
            "order_id": order_id,
            "payment_id": "PT248285AFASMFASM",
            "items" : data["items"],
            "status": "paid",
            "amount": data["totals"]["total"]
        }
    
    time.sleep(2)
    pay_collection.insert_one({
            "payment_id": payment_event["payment_id"],
            "order_id": payment_event["order_id"],
            "amount": payment_event["amount"],
            "items" : order_data["items"],
            "status": "paid",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })

        # Update order status
    # send_kafka("payment-confirmation", payment_event)
    orders.update_one(
        {"razorpay_order_id": payment_event["order_id"]},
        {"$set": {"status": "paid",
            "updated_at": datetime.utcnow()}}
    )
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    data = {
        "message": "Order created",
        "order_id": order_id,
        "razorpay_order_id": order_id,
        "amount": data["totals"]["total"],
        "currency": "INR",
        "paymentStatus" : "paid"
    }
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
    ParseDict(data,person)
    print(person.SerializeToString())
    return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")

@router.post("/orderstatus/update")
async def accept_order(request: Request,current_user: dict = Depends(get_current_user)):
    try:
        datab = await request.body()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid data")
    person = userpayload_pb2.Payload()
    person.ParseFromString(datab)
    data1 = base64.b64decode(person.payload.replace("-","+").replace("_","/")).decode()
    data = json.loads(decrypt(sha256_hash_string(data1[:20]+current_user["userid"]),hash_string(data1[-4:]+current_user["userid"]),data1[20:-4]))
    # Atomic update: only assign if not already assigned
    result = await delivery.find_one_and_update(
        {
            "orderId": data["order_id"],
            "status": "pending"  # Only allow if not assigned
        },
        {
            "$set": {
                "status": data["status"],
                "delivery_partner" : data["partner"],
                "assigned_at": datetime.utcnow()
            }
        },
        return_document=True  # Return the updated doc if successful
    )
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    if result:
        # Success - return confirmation
        data= {
            "message": "Order assigned successfully",
            "order_id": str(result["_id"]),
            "assigned_to": result["delivery_partner"]
        }
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Success","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")
    else:
        # Assignment failed - someone else took it
        data = {"data":"Order already assigned"}
        enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
        data = {"message": "Failed","payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")}
        ParseDict(data,person)
        print(person.SerializeToString())
        return Response(status_code=409,content=person.SerializeToString(), media_type="application/x-protobuf")
    
# @router.get("/myorders")
# async def myorders(data: Request,current_user: dict = Depends(get_current_user)):
    


import asyncio
from aiokafka import AIOKafkaConsumer
import json

# async def run_delivery_processor():
#     TOPIC_DELIVERY_STATUS = "delivery-status"
#     consumer = AIOKafkaConsumer(
#         TOPIC_DELIVERY_STATUS,
#         bootstrap_servers="localhost:9092",
#         group_id='delivery-processor-group',
#         auto_offset_reset='earliest',
#         value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#     )

#     await consumer.start()
#     try:
#         async for message in consumer:
#             order_status_event = message.value
#             print(order_status_event)
#             order_id = order_status_event['order_id']
#             status = order_status_event['status']
#             partner = order_status_event['partner']
#             delivery_event = {
#                 "status": status,
#                 "delivery_partner": partner
#             }
#             # Update MongoDB document for the order
#             await delivery.update_one(
#                 {"orderId": order_id},
#                 {"$set": delivery_event}
#             )
#             print(f"[DeliveryProcessor] Order {order_id} is {status}")

#     except Exception as e:
#         print("Error in Kafka consumer:", str(e))
#     finally:
#         await consumer.stop()

async def handle_payment_confirmation():
    consumer = AIOKafkaConsumer(
        'payment-confirmation',
        bootstrap_servers="localhost:9092",
        group_id='inventory-updated',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    await consumer.start()
    print("📦 Delivery Service listening for payment confirmations...")

    try:
        async for msg in consumer:
            event = msg.value
            print(f"✅ Payment confirmed for order {event['order_id']}")

            # Update inventory (async)
            for item in event["items"]:
                result = await prod_collection.update_one(
                    {"productId": item["product_id"]},
                    {
                        "$inc": {
                            "availableSizes.$[elem].stock": -item["quantity"]
                        }
                    },
                    array_filters=[{"elem.size": item["size"]}]
                )
                print(f"Updated stock for product {item['product_id']}, matched: {result.matched_count}, modified: {result.modified_count}")

            # Insert delivery document (async)
            await delivery.insert_one({
                "orderId": event["order_id"],
                "status": "pending",
                "shippingProvider": "ShipRocket",
                "delivery_partner": ""
            })

    except Exception as e:
        print("Error in Kafka consumer:", str(e))
    finally:
        await consumer.stop()

# To run this coroutine, from an async context:
# asyncio.run(handle_payment_confirmation())

    
@router.get("/mybags")
async def bags(
    current_user: dict = Depends(get_current_user)
):
    cursor = await cart_collection.find_one({"userid": current_user["userid"]})
    print(cursor["items"])
    documents = []
    for doc in cursor["items"]:
        try:
            documents.append({"name" : doc["name"],"image" : doc["image"],"price" : doc["price"],"quantity" : doc["quantity"],"product_id" : doc["product_id"] })
        except:
            pass
    data = {"assets" : documents}
    print(data)
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data))+iv
    person = userpayload_pb2.Payload()
    data =  {
        "message": "Success",
        "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    ParseDict(data,person)
    return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")
    

@router.get("/myorders")
async def bags(
    current_user: dict = Depends(get_current_user)
):
    cursor = orders.find({"user_id": "sboVAtf7GTgq"})  # Your filter here
    clean_items = []

    async for doc in cursor:
        cleaned_doc = {
            k: v for k, v in doc.items()
            if k != "_id" and not isinstance(v, datetime)
        }
        clean_items.append(cleaned_doc)
    for x in clean_items:
        for y in x["items"]:
            data = await prod_collection.find_one({"productId": y["product_id"]})
            y["store_name"] = data["store_name"]
            y["fit"] = data["fit"]
            y["gender"] = data["gender"]

    data2 = {"orders" : clean_items}
    key = generate_random_text(length=20)
    iv = generate_random_text(length=4)
    enc = key+encrypt(sha256_hash_string(key+current_user["userid"]),hash_string(iv+current_user["userid"]),json.dumps(data2))+iv
    person = userpayload_pb2.Payload()
    data =  {
        "message": "Success",
        "payload": base64.b64encode(enc.encode()).decode().replace("+","-").replace("/","_")
    }
    ParseDict(data,person)
    return Response(status_code=200,content=person.SerializeToString(), media_type="application/x-protobuf")

import asyncio

# async def start_consumer2():
#     await run_delivery_processor()

# async def start_consumer():
#     await handle_payment_confirmation()

# In FastAPI startup event or other async context:
# asyncio.create_task(start_consumer())
# asyncio.create_task(start_consumer2())