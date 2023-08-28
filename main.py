import os
from typing import Annotated, Optional
from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
import uvicorn
from db.schemas import NfeQueryParams
from db.sql import get_general, get_session

# from db.schemas import NfeQueryParams

# from db.sql import insert_xml_from_folder
from hotel_api.tasks import novas_notas, test, test_nota
import logging


for name, logger in logging.root.manager.loggerDict.items():
    logger.disabled = False
logger = logging.getLogger("uvicorn")
# c_handler = logging.StreamHandler()
# logger.addHandler(c_handler)
# c_handler.setFormatter(logging.Formatter('%(name)s %(levelname)s - %(message)s'))
# logger.setLevel(logging.INFO)
# c_handler.setLevel(logging.INFO)


app = FastAPI()
cwd = os.getcwd()


@app.get("/", status_code=202, tags=["info"])
async def root():
    logger.info("Calling /")
    ult_nsu, xmls, notas = test.delay().get()
    # logger.info("%s", notas)
    # logger.info("%s", ult_nsu)
    return {"message": "Adding files to database"}


@app.get("/novas_notas", status_code=202, tags=["info"])
async def novas():
    logger.info("Calling /novas_notas")
    ult, xmls, notas = novas_notas.delay().get()
    logger.info("XMLS: \n%s", xmls)
    # logger.info("%s", ult_nsu)
    return {"message": "getting novas notas to database"}


@app.get("/filtrar_notas", status_code=202, tags=["info"])
async def filtrar_notas(params: NfeQueryParams = Depends()):
    logger.info("Calling /")
    session = get_session()
    if params.nome:
        params.nome = params.nome.upper()
    if params.start_date is not None and params.end_date is not None:
        if params.start_date > params.end_date:
            raise HTTPException(
                status_code=400,
                detail="Bad request. Start_date must be before End_date",
            )
    query = get_general(session, params)
    dictret = dict(params.__dict__)
    dictret.pop("_sa_instance_state", None)
    return query


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
