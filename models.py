from pydantic import BaseModel, EmailStr
from typing import Optional

class UserRegister(BaseModel):
    message: str
    payload: str

class ProdData(BaseModel):
    message: str
    payload: str

class UserLogin(BaseModel):
    message: str
    payload: str

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserInDB(BaseModel):
    email: EmailStr
    username: str
    hashed_password: str
    role: str = "user"
    is_verified: bool = False
