 %(name)s - %(levelname)s - %(message)s", datefmt="%d/%m/%Y %H:%M:%S")
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)

########################################################CONEXÃO EPM#####################################################
# nome (ou IP) da maquina onde esta executando o EPM Server
epm_server = 'EMP'

# nome do usuario e senha para a sessao
epm_user = "operacao"
epm_password = "operacao"

try:
    # logger.info("Estabelecendo conexão com o servidor EPM...")
    connection = epm.EpmConnection("http://EPM:44333", "http://EPM:44332", epm_user, epm_password)
    # logger.info("Conexao bem sucedida.")
except Exception as e:
    logger.info("Erro de conexão.")
    logger.error("Detalhes: " + str(e))
    exit(1)

'''
########################################################BOT#############################################################
'''
# token = '5055652178:AAHWeA9TzlFuowZ6m_tfJKzIL9hVJCMdRJY'
# bot = telebot.TeleBot(token)
# ChatID = -1001647248143  # -1001639056155 Grupo PIMS
x = 1
diaanarm=0
voicemsg = "Oi"
''''
########################################################################################################################
# Função para buscar tags no EPM.
# Argumentos: conn: objeto EPM Connection, tags: dicionário na forma [tag:descrição], agg: objeto EPM Aggregate Details#
########################################################################################################################
'''


def get_epm_tags(conn, tag_dict, agg, query_period):
    # Tenta executar a consulta

    data = dict()

    try:
        ###logger.info("Executando a consulta aos dados...")
        data_objs = conn.getDataObjects(list(tag_dict.keys()))

        for obj in data_objs:
            data[obj] = data_objs[obj].historyReadAggregate(agg, query_period)

    ###logger.info("Consulta executada com sucesso.")

    except Exception as e:
        connection.close()
        logger.info("Erro na consulta aos dados.")
        logger.error("Detalhes: " + str(e))
    finally:
        return data


'''
########################################################################################################################
# Função para gerar Eventos
########################################################################################################################
'''
while x == 1:
    try:

        # Definicao de um periodo para consulta
        end_date = dt.datetime.now(tz=pytz.UTC)
        ini_date = end_date - dt.timedelta(minutes=1)
        periodo = epm.QueryPeriod(ini_date, end_date)
        time_interval = dt.timedelta(seconds=1)

        diaat = end_date
        diaat = diaat - dt.timedelta(hours=4)
        diaan = diaat - dt.timedelta(minutes=5)
        diaat = int(diaat.strftime('%d'))
        diaan = int(diaan.strftime('%d'))

        agg_details = epm.AggregateDetails(time_interval, epm.AggregateType.Interpolative)

        # criar df a partir do csv file
        # https://www.delftstack.com/pt/howto/python-pandas/how-to-iterate-through-rows-of-a-dataframe-in-pandas/

        df_csv = pd.read_csv("Monitoramento.csv", sep=";")

        # Consulta kks

        for i in df_csv.index:

            try:

                xkks = df_csv.iloc[i, 0]
                xkks = str(xkks)
                xtipo = df_csv.iloc[i, 1]
                try:
                    xlimite1 = df_csv.iloc[i, 2]
                    xlimite1 = float(xlimite1)
                except Exception:
                    if xlimite1 == "False":
                        xlimite1 = 0
                    xlimite1 = bool(xlimite1)
                try:
                    xlimite2 = df_csv.iloc[i, 3]
                    xlimite2 = float(xlimite2)
                except Exception:
                    if xlimite2 == "False":
                        xlimite2 = 0
                    xlimite2 = bool(xlimite2)

                xUN = df_csv.iloc[i, 4]
                xUN = str(xUN)
                xdkks = df_csv.iloc[i, 5]
                xdkks = str(xdkks)
                xatuado = df_csv.iloc[i, 6]
                xatuado = int(xatuado)
                xdkks2 = df_csv.iloc[i, 7]
                xdkks2 = str(xdkks2)
                xkks2 = df_csv.iloc[i, 8]
                xkks2 = str(xkks2)
                xcont = df_csv.iloc[i, 9]
                xcont = int(xcont)

                if diaat - diaan != 0 and diaanarm!=diaan:
                    diaanarm=diaan
                    for i in df_csv.index:
                        df_csv.iloc[i, 9] = 0
                    df_csv.to_csv("Monitoramento.csv", index=False, sep=";")

                # Lista de tags para o relatorio
                tags0 = {xkks: xdkks, xkks2: xdkks2}
                # Cria dados:

                data_epm = get_epm_tags(connection, tags0, agg_details, periodo)

                d = dict()
                for key in data_epm.keys():
                    d[key] = data_epm[key]["Value"]
                dates = pd.date_range(str(data_epm[list(tags0.keys())[0]]['Timestamp'][0]),
                                      periods=len(data_epm[list(tags0.keys())[0]]), freq='s')
                dates = dates.tz_convert(tz='America/Manaus')
                dates = dates.tz_localize(None)

                # Cria o dataframe com os dados
                df = pd.DataFrame(d, index=dates)

                try:
                    xvariavel1 = df.iloc[59, 0]
                    xvariavel1 = round(xvariavel1, 3)
                except Exception:
                    xvariavel1 = xvariavel1
                try:
                    xvariavel2 = df.iloc[0, 0]
                    xvariavel2 = round(xvariavel2, 3)
                except Exception:
                    xvariavel2 = xvariavel2

                xlinhacsv = i

                if (xtipo == "DELTA"):

                    DeltaAbs = abs(xvariavel1 - xvariavel2)
                    Delta = xvariavel1 - xvariavel2
                    Delta = round(Delta, 3)

                    if (DeltaAbs > xlimite1):
                        if xatuado == 0:
                            df_csv.iloc[xlinhacsv, 6] = 1
                            xcont = xcont + 1
                            df_csv.iloc[xlinhacsv, 9] = xcont
                            df_csv.to_csv("Monitoramento.csv", index=False, sep=";")
                            if xcont == 6:
                                voicemsg = "Atenssaum operassaum"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = xdkks + ", com variação no último minuto"  # de " + str(Delta) + xUN + " no último minuto"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = "inspecionar o equipamento, com urgencia"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                            if xcont < 6:
                                voicemsg = xdkks + ", com variação no último minuto"  # de " + str(Delta) + xUN + " no último minuto"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                                  timeout=120,
                                                  encoding='utf-8')

                    if ((DeltaAbs < xlimite2) and (xatuado == 1)):
                        df_csv.iloc[xlinhacsv, 6] = 0
                        df_csv.to_csv("Monitoramento.csv", index=False, sep=";")

                        if xcont < 6:
                            voicemsg = xdkks + ", com valor normalizado"
                            msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                              timeout=120,
                                              encoding='utf-8')

                if (xtipo == "MAIOR"):

                    if (xvariavel1 > xlimite1):
                        if xatuado == 0:
                            df_csv.iloc[xlinhacsv, 6] = 1
                            xcont = xcont + 1
                            df_csv.iloc[xlinhacsv, 9] = xcont
                            df_csv.to_csv("Monitoramento.csv", index=False, sep=";")
                            if xcont == 5:
                                voicemsg = "Atenssaum operassaum"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = xdkks + ", com valor alto"  # + str(xvariavel1) + xUN
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = "inspecionar o equipamento, com urgencia"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                            if xcont < 5:
                                voicemsg = xdkks + ", com valor alto"  # + str(xvariavel1) + xUN
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                                  timeout=120,
                                                  encoding='utf-8')

                    if (xvariavel1 < xlimite2) and (xatuado == 1):
                        df_csv.iloc[xlinhacsv, 6] = 0
                        df_csv.to_csv("Monitoramento.csv", index=False, sep=";")

                        if xcont < 5:
                            voicemsg = xdkks + ", com valor normalizado"
                            msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                              timeout=120,
                                              encoding='utf-8')

                if (xtipo == "MENOR"):

                    if (xvariavel1 < xlimite1):
                        if xatuado == 0:
                            df_csv.iloc[xlinhacsv, 6] = 1
                            xcont = xcont + 1
                            df_csv.iloc[xlinhacsv, 9] = xcont
                            df_csv.to_csv("Monitoramento.csv", index=False, sep=";")
                            if xcont == 5:
                                voicemsg = "Atenssaum operassaum"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = xdkks + ", com valor baixo"  # + str(xvariavel1) + xUN
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = "inspecionar o equipamento, com urgencia"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                            if xcont < 5:
                                voicemsg = xdkks + ", com valor baixo"  # + str(xvariavel1) + xUN
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                                  timeout=120,
                                                  encoding='utf-8')

                    if (xvariavel1 > xlimite2) and (xatuado == 1):
                        df_csv.iloc[xlinhacsv, 6] = 0
                        df_csv.to_csv("Monitoramento.csv", index=False, sep=";")

                        if xcont < 5:
                            voicemsg = xdkks + ", com valor normalizado"
                            msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                              timeout=120,
                                              encoding='utf-8')

                if (xtipo == "BIN"):

                    if (xvariavel1 == xlimite1):
                        if xatuado == 0:
                            df_csv.iloc[xlinhacsv, 6] = 1
                            xcont=xcont+1
                            df_csv.iloc[xlinhacsv, 9] = xcont
                            df_csv.to_csv("Monitoramento.csv", index=False, sep=";")
                            if xcont == 5:
                                voicemsg = "Atenssaum operassaum"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = xdkks + ", ativado"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                                voicemsg = "inspecionar o equipamento, com urgencia"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True,
                                                      capture_output=True,
                                                      timeout=120,
                                                      encoding='utf-8')
                            if xcont < 5:
                                voicemsg = xdkks + ", ativado"
                                msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                                  timeout=120,
                                                  encoding='utf-8')

                    if (xvariavel1 == xlimite2) and (xatuado == 1):
                        df_csv.iloc[xlinhacsv, 6] = 0
                        df_csv.to_csv("Monitoramento.csv", index=False, sep=";")

                        if xcont < 5:
                            voicemsg = xdkks + ", desativado"
                            msgv = subprocess.run(['python', "Audio.py", f'{voicemsg}'], text=True, capture_output=True,
                                              timeout=120,
                                              encoding='utf-8')
            finally:
                #connection.close()
                continue
    finally:
        #connection.close()
        continue