from datetime import datetime
import logging
from celery import Celery
import celery
from sefaz.distNfe import distNfe
from sefaz.xml_parser import get_tags
from db.model import Nfe
from db.sql import Session, get_session
from lxml import etree
import os
from celery.utils.log import get_task_logger
from sefaz.distNfe import distNfe

logger = get_task_logger(__name__)

app = Celery(
    "tasks", backend=os.environ["BACKEND_URL"], broker=os.environ["BROKER_URL"]
)
cwd = os.getcwd()

app.conf.update(
    task_serializer="json",
    accept_content=["json"],  # Ignore other content
    result_serializer="json",
    timezone="America/Sao_Paulo",
    enable_utc=False,
)


@celery.signals.after_setup_logger.connect
def on_after_setup_logger(**kwargs):
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.WARNING)
    c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(c_format)
    logger.addHandler(handler)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    h1_3min = 3780
    sender.add_periodic_task(h1_3min, novas_notas.s(), name="Ler novas notas")


@app.task
def novas_notas():
    CODIGO_REJEICAO = 201
    with open(os.path.join(cwd, "celery", "ultNSU.txt"), "r") as f:
        nsu = int(f.readline().strip())
    ult_nsu = nsu
    max_nsu = float("inf")
    xmls = []
    notas = []
    while ult_nsu < max_nsu:
        ult_nsu, max_nsu, codigo, xmls = distNfe(
            "", ult_nsu, is_nsu=True, is_nsu_especifico=False
        )
        logger.info("ULT_NSU: %s MAX_NSU: %s CODIGO_RET: %s", ult_nsu, max_nsu, codigo)
        if codigo >= CODIGO_REJEICAO:
            logger.warning("ALGUM ERRO OCORREU SEFAZ. CODIGO DE RETORNO: %s", codigo)
        if ult_nsu != 0 and codigo < CODIGO_REJEICAO:
            write_ult_nsu(ult_nsu)
            for xml in xmls:
                nota = get_tags(xml_str=xml)
                notas.append(nota)
                path = write_xml(xml, nota)
                if nota is not None:
                    session = get_session()
                    try:
                        logger.warning("NOTA REPR: %s", repr(nota))
                        session.merge(nota)
                        session.commit()
                    finally:
                        session.close()
                    # with Session() as session:
                    #     session.add(nota)
    return ult_nsu, xmls, notas


def write_xml(xml: str, nota: Nfe):
    if nota is not None:
        folder = nota.get_folder()
        path = os.path.join(cwd, "xml", folder, f"{nota.chave}.xml")
    else:
        folder = "outros"
        path = os.path.join(cwd, "xml", folder, f"{datetime.now()}.xml")
    resposta = etree.fromstring(xml)
    resposta.getroottree().write(path, pretty_print=True)
    return path


def write_ult_nsu(nsu: int):
    with open(os.path.join(cwd, "celery", "ultNSU.txt"), "w") as f:
        f.write(str(nsu))


@app.task
def test():
    CODIGO_REJEICAO = 201
    ult_nsu = 4230
    chave = "35230600221990000198550010001882981020222272"
    max_nsu = float("inf")
    xmls = []
    notas = []
    ult_nsu, max_nsu, codigo, xmls = distNfe(
        chave, ult_nsu, is_nsu=False, is_nsu_especifico=True
    )
    logger.info("ULT_NSU: %s MAX_NSU: %s CODIGO_RET: %s", ult_nsu, max_nsu, codigo)
    if codigo >= CODIGO_REJEICAO:
        logger.warning("ALGUM ERRO OCORREU SEFAZ. CODIGO DE RETORNO: %s", codigo)
    if ult_nsu != 0 and codigo < CODIGO_REJEICAO:
        for xml in xmls:
            nota = get_tags(xml_str=xml)
            notas.append(nota)
            path = write_xml(xml, nota)
            print("PAth: ", path)
            logger.warning("NOTA REPR: %s", repr(nota))
            if nota is not None:
                session = get_session()
                try:
                    print("ADDING NOTA")
                    print(repr(nota))
                    session.add(nota)
                    session.commit()
                finally:
                    session.close()
                # with Session() as session:
                #     session.add(nota)
    return ult_nsu, xmls, notas


@app.task
def test_nota():
    chave = "35230624614269000126550010003386141336538648"
    ult_nsu, max_nsu, codigo, xmls = distNfe(
        chave, 4090, is_nsu=True, is_nsu_especifico=True
    )
    logger.info("ULT_NSU: %s  MAX_NSU: %s", ult_nsu, max_nsu)
    return ult_nsu, xmls
