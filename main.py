from datetime import datetime
import os
from typing import Annotated, Optional
from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from db.schemas import NfeQueryParams, UserRequest, UserResponse
from db.sql import (
    get_general,
    get_session,
    insert_xml_from_folder,
    update_notas_desbravador,
    login_username,
    login_token,
)

# from db.schemas import NfeQueryParams

# from db.sql import insert_xml_from_folder
from hotel_api.tasks import (
    download_completa,
    manifestar,
    novas_notas,
    novos_certificados,
    test_get_chave,
)
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

origins = [
    "http://pedro.muniz.carlo-fuji.nord",
    "https://pedro.muniz.carlo-fuji.nord",
    "http://pedro.muniz.carlo-fuji.nord:5173",
    "https://pedro.muniz.carlo-fuji.nord:5173",
    "http://localhost:5173",
]

orgins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/filtrar_notas", status_code=202, tags=["info"])
async def filtrar_notas(params: NfeQueryParams = Depends()):
    logger.info("Calling /filtra_notas")
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


@app.post("/mudar_desbravador_por_chave", status_code=202, tags=["change"])
async def mudar_desbravador_por_chave(
    chaves_list: Annotated[str, "Lista separada por virgulas de chaves para mudar"]
):
    return update_notas_desbravador(chaves_list.split(","))


@app.post("/login", status_code=202, tags=["login"])
async def login(login_request: UserRequest) -> UserResponse:
    user = None
    login_info, token = login_request.login_info, login_request.token
    if login_info is None and token is None:
        return UserResponse(
            success=False, token=None, err="Nenhuma informação de login recebida"
        )
    if token is not None:
        user = login_token(token)
    elif login_info is not None:
        user = login_username(login_info.username, login_info.hash_password)
    if user is None:
        return {
            "success": False,
            "err": "Não foi possível identicar o usuário",
        }
    else:
        return {
            "success": True,
            "token": user.token,
            "admin": True if user.admin else None,
        }


# @app.post("/add_user", status_code=202, tags=["login"])
async def add_user(login_request: UserRequest) -> UserResponse:
    user = None
    login_info, token = login_request.login_info, login_request.token
    if token is None:
        return UserResponse(
            success=False,
            token=None,
            err="Deve se passar token ativo para essa operação",
        )
    else:
        user = login_token(token)
    if user is None:
        return {
            "success": False,
            "err": "Não foi possível identicar o usuário",
        }
    # Continuar aqui adicionar

    # else:
    # return {
    #     "success": True,
    #     "token": user.token,
    # }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
