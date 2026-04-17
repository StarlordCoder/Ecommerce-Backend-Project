from passlib.context import CryptContext
from jose import jwt, JWTError
from datetime import datetime, timedelta
import base64,random,json,time,hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

SECRET_KEY = "secret@123"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440
COMMON_KEY = "A$R1P12H7N" +"A$R1P12H7N"[::-1]+"A$R1P12H7N"+"J2"
COMMON_KEY = COMMON_KEY
COMMON_IV = "0000000000000000"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def generate_random_text(length=10):
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
    return encrypted

def decrypt(key,iv,encrypted):
    # Decode from base64
    encrypted_bytes = base64.b64decode(encrypted.replace("-","+").replace("_","/"))
    # Extract IV and ciphertext
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # Decrypt and unpad
    pt = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
    print(pt)
    return pt.decode('utf-8')

def hash_password(password: str):
    return pwd_context.hash(password)

def serialize_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
    
# from kafka import KafkaProducer
# import json

# KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
# ORDERS_TOPIC = 'orders'
# PAYMENT_CONFIRMATION_TOPIC = 'payment-confirmation'

# # Kafka producer setup with kafka-python
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# def send_kafka(topic, data):
#     producer.send(topic, value=data)
#     producer.flush()

