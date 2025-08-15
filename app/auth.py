# app/auth.py
from datetime import datetime, timedelta
from typing import Optional, Tuple
import os, secrets, jwt

from fastapi import APIRouter, Depends, HTTPException, Response, Header, Cookie, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from passlib.hash import argon2
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    create_engine, select, delete, func, desc, and_
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ===== Config (.env) =====
JWT_SECRET = os.getenv("JWT_SECRET", secrets.token_hex(32))
JWT_EXPIRE_MIN = 30
REFRESH_EXPIRE_DAYS = 30
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "CHANGE_ME")

# Token compartilhado
ALLOW_SHARED = os.getenv("ALLOW_SHARED_TOKEN", "0") == "1"
SHARED_TOKEN = os.getenv("SHARED_TOKEN") or ADMIN_API_KEY

ACTIVE_WINDOW_SECONDS = 90
STRICT_SINGLE_DEVICE = True  # só vale para JWT normal

DATABASE_URL = os.getenv("AUTH_DB_URL", "sqlite:///./auth.db")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

# ===== Tabelas =====
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    is_admin  = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Refresh(Base):
    __tablename__ = "refresh_tokens"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True, nullable=False)
    device_id = Column(String(255), nullable=False)
    token = Column(String(255), unique=True, index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class ActiveSession(Base):
    __tablename__ = "active_sessions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True, nullable=False)
    device_id = Column(String(255), nullable=False)
    sid = Column(String(64), unique=True, index=True, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ===== Schemas =====
class AdminCreateUser(BaseModel):
    email: EmailStr
    password: str
    is_admin: Optional[bool] = False
    is_active: Optional[bool] = True

class AdminUpdateUser(BaseModel):
    password: Optional[str] = None
    is_admin: Optional[bool] = None
    is_active: Optional[bool] = None

class LoginIn(BaseModel):
    email: EmailStr
    password: str
    device_id: str

class TokenOut(BaseModel):
    access_token: str
    session_id: str
    token_type: str = "Bearer"

class UserOut(BaseModel):
    id: int
    email: EmailStr
    is_admin: bool
    is_active: bool
    created_at: datetime

security = HTTPBearer()
router = APIRouter(prefix="/auth", tags=["auth"])

# ===== Helpers =====
def db():
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()

def make_access_token(user_id: int, device_id: str, sid: str) -> str:
    payload = {"sub": user_id, "dev": device_id, "sid": sid,
               "exp": datetime.utcnow() + timedelta(minutes=JWT_EXPIRE_MIN),
               "iat": datetime.utcnow()}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def set_refresh_cookie(resp: Response, token: str):
    resp.set_cookie(
        key="refresh_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="Lax",
        max_age=REFRESH_EXPIRE_DAYS * 86400,
        path="/auth",
    )

def require_admin(x_admin_key: str = Header(..., alias="X-Admin-Key")):
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(403, "forbidden")

# ===== Admin =====
@router.post("/admin/create", dependencies=[Depends(require_admin)])
def admin_create_user(body: AdminCreateUser, s=Depends(db)):
    if s.scalar(select(User).where(User.email == body.email)):
        raise HTTPException(409, "email already exists")
    u = User(
        email=body.email,
        password_hash=argon2.hash(body.password),
        is_admin=bool(body.is_admin),
        is_active=bool(body.is_active),
    )
    s.add(u); s.commit()
    return {"ok": True, "id": u.id}

@router.get("/admin/users", dependencies=[Depends(require_admin)])
def admin_list_users(q: str = "", page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=200), s=Depends(db)):
    stmt = select(User)
    if q:
        stmt = stmt.where(User.email.ilike(f"%{q}%"))
    total = s.scalar(select(func.count()).select_from(stmt.subquery()))
    rows = s.execute(stmt.order_by(desc(User.id)).offset((page-1)*size).limit(size)).scalars().all()
    return {
        "items": [UserOut(id=u.id, email=u.email, is_admin=u.is_admin, is_active=u.is_active, created_at=u.created_at).dict() for u in rows],
        "total": total, "page": page, "size": size
    }

@router.patch("/admin/users/{user_id}", dependencies=[Depends(require_admin)])
def admin_update_user(user_id: int, body: AdminUpdateUser, s=Depends(db)):
    u = s.get(User, user_id)
    if not u: raise HTTPException(404, "user not found")
    if body.password:
        u.password_hash = argon2.hash(body.password)
    if body.is_admin is not None:
        u.is_admin = bool(body.is_admin)
    if body.is_active is not None:
        u.is_active = bool(body.is_active)
    s.commit()
    return {"ok": True}

# ===== Login / Sessões =====
@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, resp: Response, s=Depends(db)):
    u = s.scalar(select(User).where(User.email == body.email))
    if not u or not u.is_active or not argon2.verify(body.password, u.password_hash):
        raise HTTPException(401, "invalid credentials")

    # Token compartilhado: devolve o mesmo token para todos
    if ALLOW_SHARED and SHARED_TOKEN:
        set_refresh_cookie(resp, secrets.random_urlsafe(32))
        return TokenOut(access_token=SHARED_TOKEN, session_id="shared")

    cutoff = datetime.utcnow() - timedelta(seconds=ACTIVE_WINDOW_SECONDS)

    if STRICT_SINGLE_DEVICE:
        other = s.scalar(
            select(ActiveSession).where(
                and_(
                    ActiveSession.user_id == u.id,
                    ActiveSession.device_id != body.device_id,
                    ActiveSession.last_seen >= cutoff,
                )
            )
        )
        if other:
            raise HTTPException(423, detail={"reason": "active_session"})

    s.execute(delete(ActiveSession).where(ActiveSession.user_id == u.id))
    s.execute(delete(Refresh).where(Refresh.user_id == u.id, Refresh.device_id == body.device_id))

    rt = Refresh(
        user_id=u.id,
        device_id=body.device_id,
        token=secrets.token_urlsafe(48),
        expires_at=datetime.utcnow() + timedelta(days=REFRESH_EXPIRE_DAYS),
    )
    s.add(rt)

    sid = secrets.token_urlsafe(24)
    sess = ActiveSession(user_id=u.id, device_id=body.device_id, sid=sid, last_seen=datetime.utcnow())
    s.add(sess)
    s.commit()

    set_refresh_cookie(resp, rt.token)
    return TokenOut(access_token=make_access_token(u.id, body.device_id, sid), session_id=sid)

@router.post("/refresh", response_model=TokenOut)
def refresh(resp: Response, device_id: str, refresh_token: Optional[str] = Cookie(None), s=Depends(db)):
    if ALLOW_SHARED and SHARED_TOKEN:
        set_refresh_cookie(resp, secrets.random_urlsafe(32))
        return TokenOut(access_token=SHARED_TOKEN, session_id="shared")

    if not refresh_token:
        raise HTTPException(401, "no refresh token")
    rec = s.scalar(select(Refresh).where(Refresh.token == refresh_token))
    if not rec or rec.expires_at < datetime.utcnow() or rec.device_id != device_id:
        raise HTTPException(401, "invalid refresh")

    cutoff = datetime.utcnow() - timedelta(seconds=ACTIVE_WINDOW_SECONDS)
    sess = s.scalar(select(ActiveSession).where(ActiveSession.user_id == rec.user_id, ActiveSession.last_seen >= cutoff))
    if not sess:
        raise HTTPException(401, "session expired")

    s.delete(rec)
    new_rec = Refresh(
        user_id=rec.user_id,
        device_id=device_id,
        token=secrets.token_urlsafe(48),
        expires_at=datetime.utcnow() + timedelta(days=REFRESH_EXPIRE_DAYS),
    )
    s.add(new_rec); s.commit()
    set_refresh_cookie(resp, new_rec.token)
    return TokenOut(access_token=make_access_token(rec.user_id, device_id, sess.sid), session_id=sess.sid)

@router.post("/logout")
def logout(resp: Response, refresh_token: Optional[str] = Cookie(None), s=Depends(db)):
    if refresh_token:
        rec = s.scalar(select(Refresh).where(Refresh.token == refresh_token))
        if rec:
            s.execute(delete(ActiveSession).where(ActiveSession.user_id == rec.user_id))
            s.delete(rec); s.commit()
    resp.delete_cookie("refresh_token", path="/auth")
    return {"ok": True}

@router.post("/heartbeat")
def heartbeat(
    session_id: str,
    device_id: str,
    cred: HTTPAuthorizationCredentials = Depends(security),
    s=Depends(db),
):
    if ALLOW_SHARED and SHARED_TOKEN and cred.credentials == SHARED_TOKEN:
        return {"ok": True}

    try:
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(401, "invalid token")
    uid = int(payload.get("sub", 0))
    sid = str(payload.get("sid", ""))

    if sid != session_id or str(payload.get("dev", "")) != device_id:
        raise HTTPException(401, "token mismatch")

    sess = s.scalar(select(ActiveSession).where(ActiveSession.user_id == uid, ActiveSession.sid == sid))
    if not sess:
        raise HTTPException(401, "session not found")

    sess.last_seen = datetime.utcnow()
    s.commit()
    return {"ok": True}

# ===== Guard p/ SSE (query-string) =====
def _normalize_token(raw: str) -> str:
    t = (raw or "").strip().strip('"').strip("'")
    if t.lower().startswith("bearer "):
        t = t[7:].strip()
    return t

def _verify_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(401, "invalid token")

def verify_access_via_query(
    access: Optional[str] = Query(None),
    token: Optional[str] = Query(None),
    authorization: Optional[str] = Query(None),
    sid: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    device_id: Optional[str] = Query(None),
    s=Depends(db),
) -> Tuple[int, str, str]:
    acc = _normalize_token(access or token or authorization)
    sid = sid or session_id or "shared"
    device = device or device_id or "shared"

    if ALLOW_SHARED and SHARED_TOKEN and acc == SHARED_TOKEN:
        return 0, sid, device

    if not acc or not sid or not device:
        raise HTTPException(401, "missing auth")

    payload = _verify_token(acc)
    uid = int(payload.get("sub", 0))
    dev = str(payload.get("dev", ""))
    ss  = str(payload.get("sid", ""))
    if dev != device or ss != sid:
        raise HTTPException(401, "token mismatch")

    cutoff = datetime.utcnow() - timedelta(seconds=ACTIVE_WINDOW_SECONDS)
    sess = s.scalar(select(ActiveSession).where(
        ActiveSession.user_id == uid,
        ActiveSession.sid == sid,
        ActiveSession.last_seen >= cutoff
    ))
    if not sess:
        raise HTTPException(401, "session expired")
    return uid, sid, device
