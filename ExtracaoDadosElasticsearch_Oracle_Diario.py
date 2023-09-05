# -*- coding: utf-8 -*-
import utils
from datetime import datetime, date, timedelta
import configparser
from elasticsearch6 import Elasticsearch
from elasticsearch6 import exceptions
import json
import math
import cx_Oracle as cnnOracle
from valida_documentos import valida_cpf


COR_NULA: str = "\033[0m"
COR_VERMELHA: str = "\033[1;31;40m"
COR_VERDE: str = "\033[1;32;40m"
COR_AMARELA: str = "\033[1;33;40m"
COR_AZUL: str = "\033[1;34;40m"
COR_MAGENTA: str = "\033[1;35;40m"

TAMANHO_LINHA: int = 70

DELIMITADOR_CAMPO: str = '|'


def conteudo_source_hit(nome_api: str, source_hit) -> str:
    hit_formatado: str = str(hit['_id']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['received.datetime']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['organization']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['environment']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['apiproxy.name']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['apiproxy.revision']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['app.name']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['apiproduct']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['request.uri']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['url']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['responseReason']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['clientLatency']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['targetLatency']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['totalLatency']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['apigee.client_id']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['apigee.developer.app.name']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['request.header.X-Forwarded-For']) + DELIMITADOR_CAMPO + \
                         str(hit['_source']['message.status.code']) + DELIMITADOR_CAMPO

    if nome_api == 'customers-alerts-v1':
        # Tratar proxyRequest.message.content
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.content' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.content']) > 0:
                        proxyRequest_message_content = hit['_source']['proxyRequest']['message.content'] \
                                                    .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                    .replace(' ', '').replace('[', '').replace(']', '')
                        
                        proxyRequest_message_content: dict = json.loads(json.loads(json.dumps(proxyRequest_message_content)))

                        if 'networkMsisdn' in proxyRequest_message_content['data']['customer']:
                            hit_formatado += str(proxyRequest_message_content['data']['customer']['networkMsisdn']) + DELIMITADOR_CAMPO
                        else:
                            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                else:
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            else:
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
        else:
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO

        # Tratar targetResponse.message.content
        if 'targetResponse' in hit['_source']:
            if hit['_source']['targetResponse'] is not None:
                if '<html>' in hit['_source']['targetResponse']['message.content']:
                    hit_formatado += hit['_source']['targetResponse']['message.content'].replace('\n', '').replace('\r', '').replace('\t', '')
                else:
                    hit_formatado += hit['_source']['targetResponse']['message.content']
            else:
                hit_formatado += 'NULO'

    elif nome_api == 'customers-customscores-v1':
        # Tratar proxyRequest.message.content
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.headers' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.headers']) > 0:
                    proxyRequest_message_headers = hit['_source']['proxyRequest']['message.headers'] \
                                                .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                .replace(' ', '').replace('[', '').replace(']', '')

                    proxyRequest_message_headers: dict = json.loads(json.loads(json.dumps(proxyRequest_message_headers)))

                    if 'document' in proxyRequest_message_headers:
                        hit_formatado += str(proxyRequest_message_headers['document'])
                    else:
                        hit_formatado += 'NULO'
                else:
                    hit_formatado += 'NULO'
            else:
                hit_formatado += 'NULO'
        else:
            hit_formatado += 'NULO'

    elif nome_api == 'customers-scores-v1':
        # Tratar proxyRequest.message.content
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.content' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.content']) > 0:
                    proxyRequest_message_content = hit['_source']['proxyRequest']['message.content'] \
                                                .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                .replace(' ', '').replace('[', '').replace(']', '')
                    
                    proxyRequest_message_content: dict = json.loads(json.loads(json.dumps(proxyRequest_message_content)))

                    dados: dict = proxyRequest_message_content['data']['customer']
                    
                    if 'idCpf' in dados:
                        hit_formatado += str(dados['idCpf']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                else:
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            else:
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
        else:
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO

        # Tratar targetResponse.message.content
        if 'targetResponse' in hit['_source']:
            if hit['_source']['targetResponse'] is not None:
                if '<html>' in hit['_source']['targetResponse']['message.content']:
                    hit_formatado += hit['_source']['targetResponse']['message.content'].replace('\n', '').replace('\r', '').replace('\t', '')
                else:
                    hit_formatado += hit['_source']['targetResponse']['message.content']
            else:
                hit_formatado += 'NULO'

    elif nome_api == 'customers-validationsaddress-v2':
        # Tratar proxyRequest.message.content
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.content' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.content']) > 0:
                    proxyRequest_message_content = hit['_source']['proxyRequest']['message.content'] \
                                                    .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                    .replace(' ', '').replace('[', '').replace(']', '')

                    proxyRequest_message_content: dict = json.loads(json.loads(json.dumps(proxyRequest_message_content)))

                    dados: dict = proxyRequest_message_content['data']['customer']

                    if 'addressNumber' in dados:
                        hit_formatado += str(dados['addressNumber']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'addressZipCode' in dados:
                        hit_formatado += str(dados['addressZipCode']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'cpf' in dados:
                        hit_formatado += str(dados['cpf']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'networkMsisdn' in dados:
                        hit_formatado += str(dados['networkMsisdn']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO'
                else:
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            else:
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
        else:
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
      
        # Tratar targetResponse.message.content
        if 'targetResponse' in hit['_source']:
            if hit['_source']['targetResponse'] is not None:
                if '<html>' in hit['_source']['targetResponse']['message.content']:
                    hit_formatado += hit['_source']['targetResponse']['message.content'].replace('\n', '').replace('\r', '').replace('\t', '')
                else:
                    hit_formatado += hit['_source']['targetResponse']['message.content']
            else:
                hit_formatado += 'NULO'

    elif nome_api == 'customers-validationslocations-v1':
        # Tratar proxyRequest.message.headers
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.headers' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.headers']) > 0:
                    proxyRequest_message_headers = hit['_source']['proxyRequest']['message.headers'] \
                                                .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                .replace(' ', '').replace('[', '').replace(']', '')

                    proxyRequest_message_headers: dict = json.loads(json.loads(json.dumps(proxyRequest_message_headers)))

                    dados: dict = proxyRequest_message_headers

                    if 'cep' in dados:
                        hit_formatado += str(dados['cep']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'cpf' in dados:
                        hit_formatado += str(dados['cpf']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'cust' in dados:
                        hit_formatado += str(dados['cust']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'guid' in dados:
                        hit_formatado += str(dados['guid']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'lat' in dados:
                        hit_formatado += str(dados['lat']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'lng' in dados:
                        hit_formatado += str(dados['lng']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'number' in dados:
                        hit_formatado += str(dados['number']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'phone' in dados:
                        hit_formatado += str(dados['phone']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                else:
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            else:
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
        else:
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO

        # Tratar targetResponse.message.content
        if 'targetResponse' in hit['_source']:
            if hit['_source']['targetResponse'] is not None:
                if '<html>' in hit['_source']['targetResponse']['message.content']:
                    hit_formatado += hit['_source']['targetResponse']['message.content'].replace('\n', '').replace('\r', '').replace('\t', '')
                else:
                    hit_formatado += hit['_source']['targetResponse']['message.content']
            else:
                hit_formatado += 'NULO'

    elif nome_api == 'customers-validationsphones-v1':
        # Tratar proxyRequest.message.content
        if hit['_source']['message.status.code'] not in (400, 401):
            if 'proxyRequest' in hit['_source']:
                if 'message.content' in hit['_source']['proxyRequest'] and \
                    len(hit['_source']['proxyRequest']['message.content']) > 0:
                    proxyRequest_message_content = hit['_source']['proxyRequest']['message.content'] \
                                                .replace('\n', '').replace('\r', '').replace('\t', '') \
                                                .replace(' ', '').replace('[', '').replace(']', '')

                    proxyRequest_message_content: dict = json.loads(json.loads(json.dumps(proxyRequest_message_content)))

                    dados: dict = proxyRequest_message_content['data']['customer']

                    if 'networkMsisdn' in dados:
                        hit_formatado += str(proxyRequest_message_content['data']['customer']['networkMsisdn']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO

                    if 'cpf' in dados:
                        hit_formatado += str(proxyRequest_message_content['data']['customer']['cpf']) + DELIMITADOR_CAMPO
                    else:
                        hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                else:
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                    hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            else:
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
                hit_formatado += 'NULO' + DELIMITADOR_CAMPO
        else:
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO
            hit_formatado += 'NULO' + DELIMITADOR_CAMPO

        # Tratar targetResponse.message.content
        if 'targetResponse' in hit['_source']:
            if hit['_source']['targetResponse'] is not None:
                if '<html>' in hit['_source']['targetResponse']['message.content']:
                    hit_formatado += hit['_source']['targetResponse']['message.content'].replace('\n', '').replace('\r', '').replace('\t', '')
                else:
                    hit_formatado += hit['_source']['targetResponse']['message.content']
            else:
                hit_formatado += 'NULO'

    return hit_formatado


def persiste_hits(conexao: cnnOracle, cursor: cnnOracle.Cursor , tabela: str, dados: list):
    # Grava registro tabela banco ORACLE
    try:
        declaracao = 'INSERT INTO {0} VALUES ({1})'.format(tabela,
                                                           str(list(dados[0])).replace('[', ':')
                                                                              .replace('\'', '')
                                                                              .replace(', ', ', :')
                                                                              .replace(']', ''))
        crs_dados_api.executemany(declaracao, dados)
        conexao.commit()
    except conexao.Error as e:
        erro, = e.args
        print('Descrição erro: {0}'.format(erro.message))
        print('Numero de linhas persistidas: {0}'.format(cursor.rowcount))

    
# Conectando base dados Oracle
try:
    print('Conectando banco Oracle...')
    cnn_P00GG1 = cnnOracle.connect(user='usr_monet',
                                   password='qaz2wsx3edc$RFV',
                                   dsn='10.18.32.197:1521/P00GG1',
                                   encoding='UTF-8')
    print('... conectado!')

except cnnOracle.DataError as e:
    print('Erro DataError')
except cnnOracle.DatabaseError as e:
    descricao_erro = e.args()
    # cx_Oracle.DatabaseError: ORA-01008: not all variables bound
except cnnOracle.InternalError as e:
    print('Erro InternalError')
except cnnOracle.IntegrityError as e:
    print('Erro IntegrityError')
except cnnOracle.OperationalError as e:
    print('Erro OperationalError')


# with open('C:\\Users\\Z386103\\Documents\\doc_menetizacao_prsantos\\TrabalhosPauloSouto\\Dados_resumo.log', 'w') as arquivo_resumo:
with open('/home/usr_monet/extract_elastic/files/Dados_resumo_{0}.log'.format(datetime.now().strftime('%d-%m-%Y %H_%M_%S')), 'w') as arquivo_resumo:
    # define o DELIMITADOR_CAMPO do arquico CSV
    DELIMITADOR_CAMPO: str = '|'

    # Data e hora do início do processo.
    print('\n')
    print('=' * TAMANHO_LINHA)
    dataHoraInicioProcesso: datetime = datetime.now()
    print('{0:^{1}}'.format('Data e hora do ínício do processo', TAMANHO_LINHA))
    print(COR_VERDE + '{0:^{1}}'.format(dataHoraInicioProcesso.strftime('%d/%m/%Y %H:%M:%S'), TAMANHO_LINHA) + COR_NULA)
    print('=' * TAMANHO_LINHA)
    print('\n')

    # Grava dados arquivo de resumo
    arquivo_resumo.write('=' * TAMANHO_LINHA + '\n')
    arquivo_resumo.write('{0:^{1}}'.format('Data e hora do ínício do processo', TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('{0:^{1}}'.format(dataHoraInicioProcesso.strftime('%d/%m/%Y %H:%M:%S'), TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('=' * TAMANHO_LINHA + '\n')

    # 
    arquivo_resumo.write('NOME DA API' + DELIMITADOR_CAMPO + ' NOME DO ÍNDICE' + DELIMITADOR_CAMPO + 'TOTAL DE HIT\'s' + '\n')

    # Inibe cursor
    print('\033[?25h', end="")

    # Lendo arquivo de configurações
    cfg: configparser.ConfigParser = configparser.ConfigParser()
    cfg.read('/home/usr_monet/extract_elastic/Configurações_Monetização.property')
    # cfg.read('C:\\Users\\Z386103\\ScriptsPython\\pjtElasticSearch\\Configurações_Monetização.property')

    # Processa ...
    nodes: list = cfg['NOS_ELASTICSEARCH']['Nos_Elasticsearch'].split('\n')

    # API´s a processar
    apis_a_processar: dict = json.loads(cfg['APIS_A_PROCESSAR']['apis_a_processar'])

    # Configurando período
    data_inicial = datetime.now().date() - timedelta(days=1)
    data_inicial_formatada = "{0}{1}".format(data_inicial, "T03:00:00.000Z")
    print('dat_filter_format_inicio', data_inicial_formatada)

    data_final = datetime.now().date()
    data_final_formatada = "{0}{1}".format(data_final, "T02:59:59.999Z")
    print('dat_filter_format_fim', data_final_formatada)

    for api in apis_a_processar:
        # Campos API's
        colunas_api: list = list(json.loads(cfg['TITULO_X_COLUNAS_APIS'][api]).values())

        # Query para extração dos dados
        query: dict = {
                    "sort": [
                        {"client.received.start.datetime": "desc"}
                    ],
                    "_source": colunas_api,
                    "docvalue_fields": [
                        {
                        "field": "client.received.start.datetime",
                        "format": "date_time"
                        },
                        {
                        "field": "received.datetime",
                        "format": "date_time"
                        }
                    ],
                    "query": {
                        "bool": {
                        "filter": [
                            {
                            "match_phrase": {
                                "apiproxy.name": {
                                "query": api
                                }
                            }
                            },
                            {
                            "range": {
                                "client.received.start.datetime": {
                                "format": "strict_date_optional_time",
                                "gte": data_inicial_formatada,
                                "lte": data_final_formatada
                                }
                            }
                            }
                        ]
                        }
                    }
                    }


        # 
        print('=' * TAMANHO_LINHA)
        print('{0:^{1}}'.format('Processando dados da API', TAMANHO_LINHA))
        print(COR_VERMELHA + '{0:^{1}}'.format(api.upper(), TAMANHO_LINHA) + COR_NULA)
        print('=' * TAMANHO_LINHA)

        # Conectando Elasdtisearch
        es: Elasticsearch = Elasticsearch(hosts=nodes,
                                        http_auth=("FulanoDeTal",
                                                   "Credencial"))

        #
        if es.info():
            # Esconde cursor
            print('\033[?25l', end='')

            # Parâmetros SCROLL
            tempo_scroll: str = '3m'
            tamanho_scroll: int = 10000

            # Índices a processar
            indices: dict = es.indices.get('dops_monetizacao_apigeesaas-' + str(data_inicial).replace('-', '.'))

            for indice in indices.keys():
                if int(indice[-2:]) > 0:
                    # Recupera dados
                    search_results: dict = es.search(index=indice,
                                                     body=query,
                                                     doc_type='_doc',
                                                     scroll=tempo_scroll,
                                                     size=tamanho_scroll,
                                                     request_timeout=30)
                    
                    # Cria cursor ...
                    crs_dados_api = cnn_P00GG1.cursor()

                    # Data e hora do início do processamento do índice
                    dataHoraInicioProcessoIndice: datetime = datetime.now()
                    print('Data e hora do ínício do processamento do índice {1}{0}{2}.'.format(indice, COR_AMARELA, COR_NULA))
                    print(dataHoraInicioProcessoIndice.strftime('%d/%m/%Y %H:%M:%S'))

                    if search_results['hits']['total']['value'] > 0:
                        # Total de lotes a processar
                        total_lotes: int = math.ceil(search_results['hits']['total']['value'] / tamanho_scroll)

                        # Processando lotes do índice ...
                        print('Processando {3}{0}{5} hits em {3}{1}{5} lote{6} de informações da API {4}{2}{5}'
                            .format('{0:,.0f}'
                            .format(search_results['hits']['total']['value']).replace(',', '.'),
                            total_lotes, api, COR_VERMELHA, COR_AMARELA, COR_NULA,
                            's' if total_lotes > 1 else ''))
                        
                        percentual_processado: float = 0
                        campos: list = list(json.loads(cfg['TITULO_X_COLUNAS_APIS'][api]).keys())
                        for lote in range(1, (total_lotes + 1)):
                            lote_hits = []

                            # Processando lote(S)
                            if lote == 1:
                                # Recebe 1o. lote
                                scroll_id: str = search_results['_scroll_id']

                                # Calcula percentual a processar
                                percentual_processado += (len(search_results['hits']['hits']) / search_results['hits']['total']['value']) * 100

                                percentual_processado_formatado = str(round(percentual_processado, 2))
                                percentual_processado_formatado = percentual_processado_formatado[:percentual_processado_formatado.find('.')].rjust(2, ' ') + \
                                                                ',' + \
                                                                percentual_processado_formatado[percentual_processado_formatado.find('.') + 1 :].rjust(2, '0')
                                percentual_processado_formatado = percentual_processado_formatado.replace(' ', '')

                                print('{1}> {0}% {2}'.format(percentual_processado_formatado, COR_AZUL, COR_NULA), end='', flush=True)

                                # Processa e grava o 1o. lote de hit's por índice ...
                                for hit in search_results['hits']['hits']:
                                    #  
                                    lote_hits.append(dict(zip(campos, conteudo_source_hit(api, hit).split('|'))))

                                # Persiste hits base Oracle
                                persiste_hits(conexao=cnn_P00GG1, cursor=crs_dados_api, tabela=apis_a_processar[api], dados=lote_hits)
                            else:
                                # Recebe demais lotes
                                try:
                                    scroll_results: dict = es.scroll(scroll_id=scroll_id, scroll=tempo_scroll, request_timeout=30)
                                except exceptions.ElasticsearchException as es:
                                    print(es)

                                # Calcula percentual a processar
                                percentual_processado += (len(scroll_results['hits']['hits']) / search_results['hits']['total']['value']) * 100

                                percentual_processado_formatado = str(round(percentual_processado, 2))
                                percentual_processado_formatado = percentual_processado_formatado[:percentual_processado_formatado.find('.')].rjust(2, ' ') + \
                                                                ',' + \
                                                                percentual_processado_formatado[percentual_processado_formatado.find('.') + 1 :].rjust(2, '0')
                                percentual_processado_formatado = percentual_processado_formatado.replace(' ', '')

                                print('{1}> {0}% {2}'.format(percentual_processado_formatado, COR_AZUL, COR_NULA), end='', flush=True)

                                # Processa e grava os demais lotes de hit's por índice ...
                                for hit in scroll_results['hits']['hits']:
                                    #  
                                    lote_hits.append(dict(zip(campos, conteudo_source_hit(api, hit).split('|'))))

                                # Persiste hits base Oracle
                                persiste_hits(conexao=cnn_P00GG1, cursor=crs_dados_api, tabela=apis_a_processar[api], dados=lote_hits)

                        # Talvez não precise limpar o SCROLL
                        es.clear_scroll(scroll_id=scroll_id)
                    else:
                        # Data e hora do fim do processamento do índice
                        dataHoraFimProcessoIndice: datetime = datetime.now()
                        print('Data e hora do fim do processameto do índice.')
                        print(dataHoraFimProcessoIndice.strftime('%d/%m/%Y %H:%M:%S'))
                        print('----- NÃO EXITEM HITs À PROCESSAR -----')
                        print('Índice processado em: {0}'.format(dataHoraFimProcessoIndice - dataHoraInicioProcessoIndice), '\n')

                    # Grava log ...
                    # ... em arquivo .CSV (DadosResumo.Log)
                    arquivo_resumo.write(api + DELIMITADOR_CAMPO + indice + DELIMITADOR_CAMPO + str(search_results['hits']['total']['value']) + '\n')
                    # ... em tabela ORACLE (stg_log)
                    crs_dados_log = cnn_P00GG1.cursor()

                    try:
                        declaracao = 'INSERT INTO stg_log VALUES (:nome_indice, :nome_api, :total_hits, :data_evento)'
                        crs_dados_log.execute(declaracao, (indice, api, search_results['hits']['total']['value'], date.today()))
                        cnn_P00GG1.commit()
                    except cnn_P00GG1.DatabaseError:
                        pass

                    #
                    print()

                    # Data e hora do fim do processamento do índice
                    dataHoraFimProcessoIndice: datetime = datetime.now()
                    print('Data e hora do fim do processameto do índice.')
                    print(dataHoraFimProcessoIndice.strftime('%d/%m/%Y %H:%M:%S'))
                    print('Índice processado em: {0}'.format(dataHoraFimProcessoIndice - dataHoraInicioProcessoIndice), '\n')

                    #
                    arquivo_resumo.flush()

                    #
                    # crs_dados_api.close()

    # Mostra cursor
    print('\033[?25h', end="")

    # Data e hora do fim do processo.
    dataHoraFimProcesso: datetime = datetime.now()
    print('=' * TAMANHO_LINHA)
    print('{0:^{1}}'.format('Data e hora do fim do processo', TAMANHO_LINHA))
    print(COR_VERDE + '{0:^{1}}'.format(dataHoraFimProcesso.strftime('%d/%m/%Y %H:%M:%S'), TAMANHO_LINHA) + COR_NULA)
    print('=' * TAMANHO_LINHA)
    print('{0:^{1}}'.format('Tempo total do processamento', TAMANHO_LINHA))
    print(COR_VERDE + '{0:^{1}}'.format(str(dataHoraFimProcesso - dataHoraInicioProcesso), TAMANHO_LINHA) + COR_NULA)
    print('=' * TAMANHO_LINHA, '\n\n')

    # Grava dados arquivo de resumo
    arquivo_resumo.write('=' * TAMANHO_LINHA + '\n')
    arquivo_resumo.write('{0:^{1}}'.format('Data e hora do fim do processo', TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('{0:^{1}}'.format(dataHoraFimProcesso.strftime('%d/%m/%Y %H:%M:%S'), TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('=' * TAMANHO_LINHA + '\n')
    arquivo_resumo.write('{0:^{1}}'.format('Tempo total do processamento', TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('{0:^{1}}'.format(str(dataHoraFimProcesso - dataHoraInicioProcesso), TAMANHO_LINHA) + '\n')
    arquivo_resumo.write('=' * TAMANHO_LINHA + '\n\n')

    #
    if cnn_P00GG1:
        cnn_P00GG1.close()
