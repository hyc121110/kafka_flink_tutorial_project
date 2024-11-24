import time
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, Session

from random import randint

from json import dumps
from kafka import KafkaProducer

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres"

kafka_nodes = "kafka:9092"
myTopic = "order"

def gen_data(order):
    prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))
    my_data = {
        'category': order.category,
        'cost': order.cost,
    }
    prod.send(topic=myTopic, value=my_data)
    print(order.customer_id, order.category, order.cost, order.item_name)
    prod.flush()

try:
    time.sleep(20)

    engine = create_engine(DATABASE_URL)


    Base = declarative_base()

    Base.metadata.create_all(engine)

    
    session = Session(engine)

    class Order(Base):
        __tablename__ = "orders"
        customer_id = Column(Integer, primary_key=True)
        category = Column(String(255))
        cost = Column(Float)
        item_name = Column(String(255))

    # insert 100000 rows of dummy data
    for i in range(1,100001):
        order = Order(
            customer_id=i,
            category=f"Category {randint(1,10)}",
            cost=i * 10.5,
            item_name=f"Item {i}",
        )
        session.add(order)

    session.commit()

    orders = session.query(Order).all()
    for order in orders:
        gen_data(order)

    session.close()

except Exception as e:
    print(e)