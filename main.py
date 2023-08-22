import os
from fastapi import FastAPI, BackgroundTasks
import uvicorn
# from db.sql import insert_xml_from_folder
from hotel_api.tasks import novas_notas, test, test_nota
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


# @app.get("/", status_code=202, tags=["info"])
# async def root():
#     logger.info('Calling /')
#     ult, xmls = test_nota.delay().get()
#     logger.info("%s", xmls)
#     # logger.info("%s", ult_nsu)
#     return {"message": "Adding files to database"}

# @app.get("/novas_notas", status_code=202, tags=["info"])
# async def novas():
#     logger.info('Calling /novas_notas')
#     ult, xmls = novas_notas.delay().get()
#     logger.info("XMLS: \n%s", xmls)
#     # logger.info("%s", ult_nsu)
#     return {"message": "getting novas notas to database"}

# @app.get("/manifestar", status_code=202, tags=["info"])
# async def novas():
#     logger.info('Calling /novas_notas')
#     ult, xmls = novas_notas.delay().get()
#     logger.info("XMLS: \n%s", xmls)
#     # logger.info("%s", ult_nsu)
#     return {"message": "getting novas notas to database"}




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
