import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv('URI')

print('Ola mundo!')