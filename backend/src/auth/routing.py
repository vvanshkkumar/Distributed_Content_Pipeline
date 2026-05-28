from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
import bcrypt
from src.database import get_db
from src.models import User
from src.auth.jwt_handler import create_access_token
from pydantic import BaseModel


router = APIRouter(
    prefix='/api/auth',
    tags=['Authentication']
)


class RegisterRequest(BaseModel):
    username: str
    password: str


def hash_password(plain_password: str) -> str:
    """Converts a plain text password to a bcrypt hash."""
    pwd_bytes = plain_password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password=pwd_bytes, salt=salt)
    return hashed_password.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Checks if a plain text password matches a bcrypt hash."""
    password_bytes = plain_password.encode('utf-8')
    hashed_bytes = hashed_password.encode('utf-8')
    return bcrypt.checkpw(password_bytes, hashed_bytes)

@router.post('/register', status_code=201)
def register(
    request: RegisterRequest,
    db: Session = Depends(get_db)
):
    existing = db.query(User).filter(
        User.username == request.username
    ).first()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Username "{request.username}" is already taken.'
        )

    hashed = hash_password(request.password)

    user = User(
        username=request.username,
        hashed_password=hashed,
        is_active=True
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        'message': f'Account created for {request.username}',
        'user_id': user.id
    }


@router.post('/login')
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(
        User.username == form_data.username
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Incorrect username or password.'
        )

    if not verify_password(
        form_data.password,
        user.hashed_password
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Incorrect username or password.'
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Account is disabled.'
        )

    token = create_access_token(
        data={'sub': user.username}
    )

    return {
        'access_token': token,
        'token_type': 'bearer',
        'username': user.username
    }