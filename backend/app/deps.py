#Add the database dependency to the app.
from collections.abc import Generator
from sqlalchemy.orm import Session
from app.db.session import SessionLocal

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

#This is how our API routes will access Postgres database. The get_db function is a dependency that will be injected into the API
#routes that need access to the database. The function creates a new database session, yields it to the route, 
#and then closes the session when the route is done.