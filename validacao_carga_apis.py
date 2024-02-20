import argparse
import configparser
import requests
import json
import json
import cx_Oracle as cnnOracle

"""Devemos melhorar para tornar esse script genérico"""


def valida_alerta(url: str, end_point: str, token: str, numero_telefone: str) -> str:
    headers_api = {'Content-Type': 'application/json',
                   'X-CustomerID': 'Fulano de Tal',
                   'event-types': 'SUBSCRIPTION_CANCELED, SIMCARD_CHANGED, MSISDN_CHANGED',
                   'cache-control': 'no-cache',
                   'user-agent': 'curl/7.60.0',
                   'x-client-auth': 'Bearer {}'.format(token)}

    data_app: dict = {"data": {"customer": {"networkMsisdn": numero_telefone}}}

    resp = requests.post(url + end_point,
                         headers=headers_api,
                         data=json.dumps(data_app))

    if resp.status_code == 200:
        return 'Alerta ' + str(resp.text)
    else:
        return 'Erro: ' + str(resp.status_code)


def valida_cpf_custom_score(url: str, end_point: str, token: str, numero_cpf: str) -> str:
    headers_api = {'Accept': 'application/json',
                   'Accept-Encoding': 'identity',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
                   'x-client-auth': 'Bearer {0}'.format(token),
                   'X-CustomerID': 'Fulano de Tal',
                   'customer': cliente['sigla'],
                   'document': numero_cpf,
                   'score': cliente['nome_score']}

    data_app: dict = {}

    resp = requests.post(url + end_point,
                         headers=headers_api,
                         data=data_app)

    numero_cpf: str = '{0}.{1}.{2}-{3}'.format(numero_cpf[:3], numero_cpf[3:6], numero_cpf[6:9], numero_cpf[9:])
    if resp.status_code == 200:
        valor_score: str = str(json.loads(resp.text)['score'][cliente['nome_score']]).rjust(6, chr(32))
        return 'Número CPF: {0} / Score ({1}): {2}'.format(numero_cpf,
                                                           [tipo_score for tipo_score in
                                                            json.loads(resp.text)['score'].keys()][0],
                                                           valor_score)
    else:
        return 'Número CPF: {0} / Codigo Erro: {1}'.format(numero_cpf,
                                                           resp.status_code)


def valida_cpf_score(url: str, end_point: str, token: str, numero_cpf: str) -> str:
    headers_api = {'Content-Type': 'application/json',
                   'X-CustomerID': 'Fulano de Tal',
                   'cache-control': 'no-cache',
                   'user-agent': 'curl/7.60.0',
                   'x-client-auth': 'Bearer {0}'.format(token)}

    data_app: dict = {'data': {'customer': {'idCpf': numero_cpf}}}

    resp = requests.post(url + end_point,
                         headers=headers_api,
                         data=json.dumps(data_app))

    numero_cpf: str = data_app['data']['customer']['idCpf']
    numero_cpf: str = '{0}.{1}.{2}-{3}'.format(numero_cpf[:3], numero_cpf[3:6], numero_cpf[6:9], numero_cpf[9:])
    if resp.status_code == 200:
        valor_score: str = str(json.loads(resp.text)['data']['statsResult']['score']).rjust(4, chr(32))
        return 'Número CPF: {0} / Score: {1}'.format(numero_cpf,
                                                     valor_score)
    else:
        return 'Número CPF: {0} / Codigo Erro: {1}'.format(numero_cpf,
                                                           resp.status_code)


def valida_endereco(url: str, end_point: str,
                    token: str,
                    numero_cep: str,
                    numero_endereco: str,
                    numero_cpf: str,
                    numero_telefone: str) -> str:
    headers_api: dict = {'Content-Type': 'application/json',
                         'X-CustomerID': 'Fulano de Tal',
                         'cache-control': 'no-cache',
                         'user-agent': 'curl/7.60.0',
                         'x-client-auth': 'Bearer {0}'.format(token)}

    data_app: dict = {"data": {"customer": {"addressZipCode": numero_cep,
                                            "addressNumber": numero_endereco,
                                            "cpf": numero_cpf,
                                            "networkMsisdn": numero_telefone}}}

    resp = requests.post(url + end_point,
                         headers=headers_api,
                         data=json.dumps(data_app))

    if resp.status_code != 200:
        return 'Valida ' + str(resp.text)
    else:
        return 'Erro: ' + str(resp.text)


def valida_telefone(url: str, end_point: str, token: str, numero_telefone: str, numero_cpf: str) -> str:
    headers_api: dict = {'Content-Type': 'application/json',
                         'X-CustomerID': 'Fulano de Tal',
                         'cache-control': 'no-cache',
                         'user-agent': 'curl/7.60.0',
                         'x-client-auth': 'Bearer {0}'.format(token)}

    data_app: dict = {"data": {"customer": {"networkMsisdn": numero_telefone, "cpf": numero_cpf}}}

    resp = requests.post(url + end_point,
                         headers=headers_api,
                         data=json.dumps(data_app))

    numero_telefone: str = json.loads(resp.request.body)['data']['customer']['networkMsisdn']
    numero_cpf: str = json.loads(resp.request.body)['data']['customer']['cpf']
    if resp.status_code == 200:
        return 'Número Linha: {0} / Número CPF: {1} / {2} / {3}'.format(numero_telefone,
                                                                        numero_cpf,
                                                                        json.loads(resp.text)['data']['result'][
                                                                            'summaryMessage'],
                                                                        json.loads(resp.text)['data']['result'][
                                                                            'detailedMessage'])
    else:
        return 'Número Linha: {0} / Número CPF: {1} / Status: {2}'.format(numero_telefone,
                                                                          numero_cpf,
                                                                          resp.status_code)


def recupera_amostra() -> list:
    try:
        print('Conectando banco Oracle...')
        dados_conexao: dict = json.loads(cfg['DADOS_CONEXOES']['SERVIDOR'])

        cnn_SERVIDOR = cnnOracle.connect(user=dados_conexao['usuario'],
                                         password=dados_conexao['senha'],
                                         dsn=dados_conexao['dsn'],
                                         encoding='UTF-8')

        # Fabrica CURSOR
        crs_ = cnn_SERVIDOR.cursor()

        query = ''
        if argumentos.nome_api == 'ALERT':
            query = '''SELECT num_ntc
                       FROM {0}.{1} 
                       WHERE ROWNUM <= {2}
                       ORDER BY 1'''.format(tabela['owner'],
                                            tabela['tabela'],
                                            argumentos.total_amostra)
        elif argumentos.nome_api == 'CUSTOMSCORES':
            query = '''SELECT CPF
                       FROM {0}.{1}
                       WHERE NM_SCORE = '{2}' AND 
                             ROWNUM <= {3}
                       ORDER BY NM_SCORE'''.format(tabela['owner'],
                                                   tabela['tabela'],
                                                   cliente['nome_score'],
                                                   argumentos.total_amostra)
        elif argumentos.nome_api == 'SCORE':
            query = '''SELECT LPAD(cpf, 11, 0) AS CPF 
                       FROM {0}.{1} 
                       WHERE ROWNUM <= {2}
                       ORDER BY 1'''.format(tabela['owner'],
                                            tabela['tabela'],
                                            argumentos.total_amostra)
        if argumentos.nome_api == 'VALIDATIONS_ADDRESS':
            pass
        elif argumentos.nome_api == 'VALIDATIONS_PHONES':
            query = '''SELECT '55' + num_ntc AS NUM_NTC,
                              LPAD(num_cpf, 11, 0) AS NUM_CPF
                       FROM {0}.{1} 
                       WHERE ROWNUM <= {2}
                       ORDER BY 1'''.format(tabela['owner'],
                                            tabela['tabela'],
                                            argumentos.total_amostra)

        # Lista contendo amostra de CPF's
        crs_.execute(query)

        # Amostra
        amostra = crs_.fetchall()

        # Fecha Cursor e Conexão
        crs_.close()
        cnn_SERVIDOR.close()

        # Retorna amostra
        return amostra

    except cnnOracle.DataError as e:
        print('Erro DataError', e)
    except cnnOracle.DatabaseError as e:
        print('Erro DatabaseError', e)
    except cnnOracle.InternalError as e:
        print('Erro InternalError', e)
    except cnnOracle.IntegrityError as e:
        print('Erro IntegrityError', e)
    except cnnOracle.OperationalError as e:
        print('Erro OperationalError', e)


def recupera_amostra_teste() -> list:
    # Valida Telefone 
    amostra_test: list = [('99999977777', '99999777777'), ]

    # Valida Alerta
    """
    amostra_test: list = [('99999977777', '99999777777'), ]
    """

    # Score e Score Customizado
    """
    amostra_test: list = [('00000000000', )]
    """

    # Retorna amostra
    return amostra_test


parser = argparse.ArgumentParser(description='Valida carga de APIs')
parser.add_argument('-na', '--nome_api', help='<A definir>', type=str, default='VALIDATIONS_PHONES')
parser.add_argument('-sc', '--sigla_cliente', help='<A definir>', type=str, default='API_VALIDATIONS_PHONES')
parser.add_argument('-ta', '--total_amostra', help='<A definir>', type=int, default=25)

argumentos = parser.parse_args()

# Lendo arquivo de configuraçães
cfg: configparser.ConfigParser = configparser.ConfigParser()
# cfg.read('/home/prsantos/valida_carga_apis/Configuracoes_ValidacaoCargaAPIs.property')
cfg.read('C:\\Users\\PRSantos\\ScriptsPython\\pjtTesteStressAPIs\\Configuracoes_ValidacaoCargaAPIs.property')

cliente: dict = json.loads(cfg['CLIENTES'][argumentos.sigla_cliente])
tabela: dict = json.loads(cfg['TABELAS_APIS'][argumentos.nome_api])

# Gera Token
headers_token: dict = {'Content-Type': 'application/x-www-form-urlencoded',
                       'cache-control': 'no-cache',
                       'x-client-auth': 'Basic {0}'.format(cliente['senha'])}

data_token = {'grant_type': 'client_credentials'}
respToken = requests.post(url='https://api.claro.com.br/oauth2/v1/token',
                          headers=headers_token,
                          data=data_token)

# 
if respToken.status_code == 200:
    # Recuperando amostra de dados da base ORACLE
    amostra: list = recupera_amostra_teste()
    # amostra: list = recupera_amostra()

    # Itera sobre a lista de amostra de CPF's
    for parametro in amostra:
        if argumentos.nome_api == 'ALERT':
            print(valida_alerta(url='https://api.claro.com.br',
                                end_point='/customers/v1/alerts',
                                token=json.loads(respToken.text)['access_token'],
                                numero_telefone='55' + str(parametro[0])))
        elif argumentos.nome_api == 'CUSTOMSCORES':
            print(valida_cpf_custom_score(url='https://api.claro.com.br',
                                          end_point='/customers/v1/customscores',
                                          token=json.loads(respToken.text)['access_token'],
                                          numero_cpf=parametro[0]))
        elif argumentos.nome_api == 'SCORE':
            print(valida_cpf_score(url='https://api.claro.com.br',
                                   end_point='/customers/v1/scores',
                                   token=json.loads(respToken.text)['access_token'],
                                   numero_cpf=parametro[0]))
        elif argumentos.nome_api == 'VALIDATIONS_ADDRESS':
            print(valida_endereco(url='https://api.claro.com.br',
                                  end_point='/customers/v2/validationsaddress',
                                  token=json.loads(respToken.text)['access_token'],
                                  addressZipCode=parametro[0],
                                  addressNumber=parametro[1],
                                  numero_cpf=parametro[2],
                                  networkMsisdn=parametro[3]))
        elif argumentos.nome_api == 'VALIDATIONS_PHONES':
            print(valida_telefone(url='https://api.claro.com.br',
                                  end_point='/customers/v1/validationsphones',
                                  token=json.loads(respToken.text)['access_token'],
                                  numero_telefone='55' + str(parametro[0]),
                                  numero_cpf=parametro[1]))
else:
    print('Erro token: {0}'.format(argumentos.sigla_cliente) + ' - ' + str(respToken.status_code))
