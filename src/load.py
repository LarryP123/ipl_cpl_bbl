from sqlalchemy import create_engine

def load_data(df, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql("users", con=engine, if_exists="replace", index=False)