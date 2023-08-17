import os
from fastapi import FastAPI, BackgroundTasks
import uvicorn
# from db.sql import insert_xml_from_folder
from hotel_api.tasks import test
import logging


for name, logger in logging.root.manager.loggerDict.items():
    logger.disabled=False
logger = logging.getLogger('uvicorn')
# c_handler = logging.StreamHandler()
# logger.addHandler(c_handler)
# c_handler.setFormatter(logging.Formatter('%(name)s %(levelname)s - %(message)s'))
# logger.setLevel(logging.INFO)
# c_handler.setLevel(logging.INFO)


app = FastAPI()
cwd = os.getcwd()


@app.get("/", status_code=202, tags=["info"])
async def root():
    logger.info('Calling /')
    logger.info("%s", test.delay('HELLO WORLD').get())
    return {"message": "Adding files to database"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
