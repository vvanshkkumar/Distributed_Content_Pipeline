import os
from datetime import datetime, timedelta
from jose import JWTError, jwt
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from src.database import get_db
from src.models import User
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv(
    'JWT_SECRET_KEY',
    'changethisinsecretkeyinproduction'
)
ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
EXPIRE_HOURS = int(os.getenv('JWT_EXPIRE_HOURS', '24'))

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl='/api/auth/login'
)


def create_access_token(data: dict) -> str:
    payload = data.copy()

    expire = datetime.utcnow() + timedelta(
        hours=EXPIRE_HOURS
    )

    payload['exp'] = expire

    token = jwt.encode(
        payload,
        SECRET_KEY,
        algorithm=ALGORITHM
    )

    return token


def verify_token(token: str) -> str:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Invalid or expired token. Please log in again.',
        headers={'WWW-Authenticate': 'Bearer'},
    )

    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )

        username: str = payload.get('sub')

        if username is None:
            raise credentials_exception

        return username

    except JWTError:
        raise credentials_exception


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> User:
    username = verify_token(token)

    user = db.query(User).filter(
        User.username == username
    ).first()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='User no longer exists.',
            headers={'WWW-Authenticate': 'Bearer'},
        )

    return user