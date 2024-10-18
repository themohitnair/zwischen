from sqlmodel import create_engine, Session, SQLModel, Field

class Log(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    timestamp: str
    method: str
    ip: str    
    city: str
    country: str
    latitude: float
    longitude: float
    url: str 
    status_code: int
    browser: str
    referrer: str 
    processing_time: float 

DATABASE_FILE = "logs.db"
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"

engine = create_engine(
    url=DATABASE_URL,
    connect_args= {
        "check_same_thread": False
    },
    pool_size=60,
    max_overflow=10
)

def init_db():
    SQLModel.metadata.create_all(engine)

def yield_session():
    with Session(engine) as session:
        yield session