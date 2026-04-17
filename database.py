from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = "mongodb://localhost:27017"
client = AsyncIOMotorClient(MONGO_URI)
db = client["fastapi_auth"]
user_collection = db["users"]
prod_collection = db["products"]
bags_collection = db["custbags"]
cart_collection = db["usercart"]
pay_collection = db["payments"]
orders = db["orders"]
delivery = db["delivery"]
