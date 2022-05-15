# -*- encoding: utf-8 -*-
import time
from datetime import datetime
from selenium import webdriver as wd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (StaleElementReferenceException)


def elemento_existe(wdw, locator):
    return wdw.until(EC.presence_of_element_located(locator))


def elemento_clicavel(wdw, locator):
    return wdw.until(EC.element_to_be_clickable(locator))


def clica_elemento(wd, wdw, locator):
    # Verifica se o elemento existe
    if bool(elemento_existe(wdw, locator)):

        # Verifica se elemento clicável
        if bool(elemento_clicavel(wdw, locator)):
            wd.find_element(locator[0], locator[1]).click()


def ws_estatisticas_bet(url_principal):
    # Opções para o Driver
    options = Options()
    options.page_load_strategy = 'none'
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument('user_agent=Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36
    # (KHTML, like Gecko) Chrome/93.0.4577.63 Mobile Safari/537.36')

    # Infoca browse
    print('Iniciando driver ...')
    # with wd.Chrome(options=options) as WdChrome:
    with wd.Chrome() as WdChrome:
        # Requisitando página
        print('Requisitando página {} ...'.format(url_principal))
        WdChrome.get(url_principal)

        # Aguarda pela renderização da página
        print('Aguardando renderização da página ...')
        wdw = WebDriverWait(WdChrome, 30)

        # Aguardando receber título da página
        wdw.until(EC.title_is('Futebol: Série A 2021 - resultados, calendário - Livesport.com'))

        # Validando título da página
        wdw.until(EC.title_contains('Futebol: Série A 2021'))

        # Maximizando drive
        print('Maximizando drive ...')
        WdChrome.maximize_window()

        # Aceitando cookies
        print('Aceitando cookies ...')
        clica_elemento(WdChrome, wdw, (By.ID, 'onetrust-accept-btn-handler'))
        print('... cookies aceitos.')

        # Expandindo os confrontos da temporada
        print('Expandindo os confrontos da temporada ...')
        clica_elemento(WdChrome, wdw, (By.XPATH, '//*[@id="live-table"]/section/div[2]/div/a'))

        while True:
            try:
                clica_elemento(WdChrome, wdw, (By.XPATH, '//*[@id="live-table"]/div[1]/div/div/a'))
                print('Clicado.')
            except StaleElementReferenceException:
                break

        print('... confrontos da temporada expandidos.')

        print('Finished')
        time.sleep(15)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Data e hora do "INÍCIO" do processo.
    dataHoraInicioProcesso = datetime.now()
    print('-- Início do processo.', '\n', dataHoraInicioProcesso.strftime('%d/%m/%Y %H:%M:%S'), '\n')

    ws_estatisticas_bet('https://www.livesport.com/br/futebol/brasil/serie-a-2021/')

    # Data e hora do "FIM do processo.
    dataHoraFimProcesso = datetime.now()
    print('-- Fim do processo.', '\n', dataHoraFimProcesso.strftime('%d/%m/%Y %H:%M:%S'), '\n')
    print('-- Processo executado em : ', dataHoraFimProcesso - dataHoraInicioProcesso)
