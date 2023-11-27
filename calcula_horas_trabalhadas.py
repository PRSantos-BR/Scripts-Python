from datetime import datetime, timedelta
import locale


def calcula_segundos_trabalhados(expediente: list) -> timedelta:
    primeiro_expediente: timedelta = (datetime.strptime(expediente[0] + ' ' + expediente[2], '%d/%m/%Y %H:%M:%S') -
                                      datetime.strptime(expediente[0] + ' ' + expediente[1], '%d/%m/%Y %H:%M:%S'))

    segundo_expediente: timedelta = (datetime.strptime(expediente[0] + ' ' + expediente[4], '%d/%m/%Y %H:%M:%S') -
                                     datetime.strptime(expediente[0] + ' ' + expediente[3], '%d/%m/%Y %H:%M:%S'))

    total_expediente: timedelta = primeiro_expediente + segundo_expediente

    return total_expediente


locale.setlocale(category=locale.LC_ALL, locale='pt_BR.UTF-8')

expedientes: list = [['30/10/2023', '09:10:00', '12:00:00', '14:00:00', '18:44:00'],
                     ['31/10/2023', '09:31:00', '12:00:00', '00:00:00', '00:00:00'],
                     ['01/11/2023', '08:40:00', '12:00:00', '14:00:00', '17:45:00'],
                     ['02/11/2023', '09:15:00', '13:34:00', '00:00:00', '00:00:00'],
                     ['10/11/2023', '08:00:00', '12:00:00', '14:00:00', '17:00:00'],
                     ['13/11/2023', '08:18:00', '12:00:00', '00:00:00', '00:00:00'],
                     ['15/11/2023', '09:30:00', '12:00:00', '14:00:00', '18:00:00'],
                     ['16/11/2023', '08:40:00', '12:00:00', '14:00:00', '18:00:00'],
                     ['17/11/2023', '09:43:00', '12:00:00', '14:00:00', '18:30:00'],
                     ['18/11/2023', '09:10:00', '11:00:00', '00:00:00', '00:00:00'],
                     ['21/11/2023', '07:40:00', '11:00:00', '00:00:00', '00:00:00'],
                     ['24/11/2023', '09:20:00', '00:00:00', '00:00:00', '17:00:00']]

valor_hh: float = float(input('Forneça o valor do H/H: '))

#
segundos_trabalhados: int = sum([calcula_segundos_trabalhados(expediente).seconds
                                 for expediente
                                 in expedientes])
minutos, segundos = divmod(segundos_trabalhados, 60)
horas, minutos = divmod(minutos, 60)

print('Horas trabalhadas: {0}'.format('%02d:%02d:%02d' % (horas, minutos, segundos)))
print('Total a pagar: {0}'.format(locale.currency(val=(valor_hh / 3600) * segundos_trabalhados,
                                                  grouping=True,
                                                  symbol=True))
      )
