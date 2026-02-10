import time
import httpx
import jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer
from typing import List
from pydantic import BaseModel

from app.config.settings import settings

oauth2_scheme = HTTPBearer()

class User(BaseModel):
    login: str
    access: str  # "read" or "write"

async def get_current_user(request: Request, credentials=Depends(oauth2_scheme)) -> User:
    token = credentials.credentials
    # 1. Получить JWT через Yandex API
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://login.yandex.ru/info?format=jwt",
            headers={"Authorization": f"OAuth {token}"},
            timeout=10,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid OAuth token")
        jwt_token = resp.text
    # 2. Декодировать JWT
    try:
        payload = jwt.decode(jwt_token, options={"verify_signature": False})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid JWT token")
    # 3. Проверить exp
    if payload.get("exp", 0) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")
    login = payload.get("login")
    if not login:
        raise HTTPException(status_code=401, detail="No login in token")
    # 4. Проверить доступ
    # Если есть в write_access — полный доступ
    if login in settings.get_write_access_list():
        return User(login=login, access="write")
    if login in settings.get_read_access_list():
        return User(login=login, access="read")
    raise HTTPException(status_code=403, detail="Access denied")

def require_write(user: User = Depends(get_current_user)):
    if user.access != "write":
        raise HTTPException(status_code=403, detail="Write access required")
    return user

def require_read(user: User = Depends(get_current_user)):
    return user
