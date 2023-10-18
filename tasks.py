from datetime import datetime
import logging
from celery import Celery
import celery
from sefaz.distNfe import distNfe
from sefaz.manifestarNfe import manifestNfe
from sefaz.xml_parser import get_tags
from sefaz.utils import get_certificados
from db.model import Nfe
from db.sql import (
    Session,
    get_by_date,
    get_manifestando,
    get_session,
    add_certificados,
    read_ult_nsu,
    write_ult_nsu,
)
from lxml import etree
import os
from celery.utils.log import get_task_logger

certificados = get_certificados()

logger = get_task_logger(__name__)

app = Celery(
    "tasks", backend=os.environ["BACKEND_URL"], broker=os.environ["BROKER_URL"]
)
cwd = os.getcxwd()

app.conf.update(
    task_serializer="json",
    accept_content=["json"],  # Ignore other content
    result_serializer="json",
    timezone="America/Sao_Paulo",
    enable_utc=False,
)

CODIGO_REJEICAO = 201


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
    h1_3min = 3780  # 1 hour 3 minutes in seconds
    sender.add_periodic_task(h1_3min, novas_notas.s(), name="Ler novas notas")
    sender.add_periodic_task(
        h1_3min, download_completa.s(), name="Baixar notas completas"
    )
    sender.add_periodic_task(
        h1_3min, manifest_start_month.s(), name="Manifestar mes passado"
    )
    # sender.add_periodic_task(
    #     3600, novos_certificados.s(), name="Ler novos certificados"
    # )


@app.task
def novas_notas():
    xmls = []
    notas = []
    session = get_session()
    for cert in certificados:
        cnpj = cert.cnpj
        nsu = read_ult_nsu(session, cnpj=cnpj)
        ult_nsu = nsu
        max_nsu = float("inf")
        while ult_nsu < max_nsu:
            ult_nsu, max_nsu, codigo, xmls = distNfe(
                "", ult_nsu, is_nsu=True, is_nsu_especifico=False, Certificado=cert
            )
            logger.info(
                "ULT_NSU: %s MAX_NSU: %s CODIGO_RET: %s", ult_nsu, max_nsu, codigo
            )
            if codigo >= CODIGO_REJEICAO:
                logger.warning(
                    "ALGUM ERRO OCORREU NO SEFAZ. CODIGO DE RETORNO: %s", codigo
                )
            if ult_nsu != 0 and codigo < CODIGO_REJEICAO:
                for xml in xmls:
                    nota = get_tags(xml_str=xml)
                    notas.append(nota)
                    path = write_xml(xml, nota)
                    if nota is not None:
                        if nota.cnpj_comprador is None:
                            nota.cnpj_comprador = cnpj
                        session = get_session()
                        try:
                            logger.warning("NOTA REPR: %s", repr(nota))
                            session.merge(nota)
                            session.commit()
                            ciencia_emissao = 2
                            manifestNfe(nota.chave, ciencia_emissao, Certificado=cert)
                        finally:
                            session.close()
                        # with Session() as session:
                        #     session.add(nota)
                write_ult_nsu(session, ult_nsu, cnpj)
                
    return ult_nsu, xmls, notas


@app.task
def download_completa():
    completos = []
    for cert in certificados:
        cnpj = cert.cnpj

        session = get_session()
        session.expire_on_commit = False
        notas = get_manifestando(session)

        for nota in notas[:20]:
            ult_nsu, max_nsu, codigo, xmls = distNfe(
                nota.chave, 0, is_nsu=False, is_nsu_especifico=False, Certificado=cert
            )
            if codigo < CODIGO_REJEICAO:
                for xml in xmls:
                    completos.append(xml)
                    new_nota = get_tags(xml_str=xml)
                    if new_nota.cnpj_comprador is None:
                        new_nota.cnpj_comprador = cnpj
                    path = write_xml(xml, new_nota)
                    try:
                        logger.warning("NOTA REPR: %s", repr(new_nota))
                        session.merge(new_nota)
                        session.commit()
                    finally:
                        session.close()
    return completos


@app.task
def test_get_chave(chave: str):
    session = get_session()
    session.expire_on_commit = False
    completos = []
    ult_nsu, max_nsu, codigo, xmls = distNfe(
        chave, 0, is_nsu=False, is_nsu_especifico=False
    )
    if codigo < CODIGO_REJEICAO:
        for xml in xmls:
            completos.append(xml)
            new_nota = get_tags(xml_str=xml)
            path = write_xml(xml, new_nota)
            try:
                logger.warning("NOTA REPR: %s", repr(new_nota))
                session.merge(new_nota)
                session.commit()
            finally:
                session.close()
    return completos


@app.task
async def manifest_start_month():
    today = datetime.now()
    day = today.day
    month = today.month
    year = today.year
    if day == 1 or day == 15:
        return await manifestar(year, month)
    return []


async def manifestar(year: int, month: int):
    session = get_session()
    notas = get_by_date(session, year, month)
    CNPJ = "51548782000139"

    print(notas)
    session = get_session()
    for nota in notas:
        chave = nota.chave
        cnpj = nota.cnpj_comprador
        for cert in certificados:
            if cert.cnpj == cnpj:
                certificado = cert
        manifestNfe(chave, Certificado=certificado)  # TODO edit here to add certificate
        try:
            nota.manifestando = True
            session.merge(nota)
            session.commit()
        finally:
            session.close()

    return notas


@app.task
def novos_certificados():
    add_certificados(get_session())


def write_xml(xml: str, nota: Nfe = None):
    if nota is not None:
        folder = nota.get_folder()
        path = os.path.join(cwd, "xml", folder, f"{nota.chave}.xml")
    else:
        folder = "outros"
        path = os.path.join(cwd, "xml", folder, f"{datetime.now()}.xml")
    resposta = etree.fromstring(xml)
    resposta.getroottree().write(path, pretty_print=True)
    return path


# def write_ult_nsu(nsu: int, cnpj: str):
#     with open(os.path.join(cwd, "celery", "nsu", f"nsu_{cnpj}.txt"), "w") as f:
#         f.write(str(nsu))


# def read_ult_nsu(nsu: int, cnpj: str):
#     with open(os.path.join(cwd, "celery", "nsu", f"nsu_{cnpj}.txt"), "r") as f:
#         return f.read()
