from datetime import datetime, timedelta, timezone 
from jose import jwt  # type: ignore
from passlib.context import CryptContext # type: ignore

# JWT: JSON WEB TOKEN. Containds header, payload (expiration, username) and signature with the secret key. Server will generate it and validate requests with from users with it


SECRET_KEY = "KEY"
ALGORITHM = "HS256"

pwd_context = CryptContext(schemes=["bcrypt"])

def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain,hashed)

def create_access_token(sub: str, expires_minutes: int = 30) -> str:
    now = datetime.now(timezone.utz)
    payload = {"sub":sub,"exp":now+ timedelta(minutes=expires_minutes)}
    return jwt.encode(payload,SECRET_KEY,algorithm=ALGORITHM)

