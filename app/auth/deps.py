import time
import httpx
import jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from app.config.settings import settings

oauth2_scheme = HTTPBearer()

class User(BaseModel):
    login: str
    access: str  # "read" or "write"

async def get_current_user(request: Request, credentials=Depends(oauth2_scheme)) -> User:
    oauth_token = credentials.credentials

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://login.yandex.ru/info",
            params={
                "format": "jwt",
            },
            headers={"Authorization": f"OAuth {oauth_token}"},
            timeout=10,
        )

        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail=f"Yandex error: {resp.text}")

        jwt_token = resp.text.strip()

    # 2. Проверяем подпись и всё остальное
    try:
        payload = jwt.decode(
            jwt_token,
            settings.yandex.client_secret,
            algorithms=["HS256"],
            options={
                "require": ["exp", "iat"],
                "verify_exp": True,
            }
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="JWT expired")
    except jwt.InvalidSignatureError:
        raise HTTPException(status_code=401, detail="Invalid JWT signature")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid JWT: {str(e)}")

    login = payload.get("login")
    if not login:
        raise HTTPException(status_code=401, detail="No login in token")

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
