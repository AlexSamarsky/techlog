import logging
import graypy
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from raven import Client


client = Client('http://a5b69f794db646639939acfa7870c4ae@devops-test:9000/2')
o = {'test': 'test'}
o2 = {'fdsa': 'frewq'}
# client.context.activate()
# client.context.merge = o2
client.user_context(o)
client.extra_context(o2)
client.captureMessage('raven test error')
# client.context.clear()


# sentry_logging = LoggingIntegration(
#     level=logging.INFO,        # Capture info and above as breadcrumbs
#     event_level=logging.ERROR  # Send errors as events
# )
# sentry_sdk.init(
#     dsn="http://a5b69f794db646639939acfa7870c4ae@devops-test:9000/2",
#     integrations=[sentry_logging]
# )
# o = {'test': 'test'}
# sentry_sdk.set_context('context', o)

# logging.error('test error2')

# my_logger = logging.getLogger('test_logger')
# my_logger.setLevel(logging.DEBUG)

# handler = graypy.GELFUDPHandler('10.1.252.193', 30057)
# my_logger.addHandler(handler)

# my_logger.debug('Test Graylog message')
# o = {
#     "version": "1.1",
#     "host": "onyx-app-dev",
#     "short_message": "ВнешниеОбработки.Создание",
#     "full_message": "C:\\\\Users\\\\kosichkin_m_a\\\\AppData\\\\Local\\\\Temp\\\\v8_9F7B_7b.epf",
#     "timestamp": 1630994830,
#     "level": 1,
#     "context": "client",
#     "user": "Косичкин Максим Андреевич",
#     "owner": "МетодыУправленияОсновнымиПроцессами",
#     "duration": 138,
#     "event": "ВнешниеОбработки.Создание",
#     "app": "Srvr=\"onyx-app-dev:3541\";Ref=\"retail_own_tester_Kosichkin\";",
#     "app_type": "1C",
#     "session": 2,
#     "connection": 10,
#     "timezone": "Europe/Moscow",
#     "configuration_1C": "Розница"
# }




# import logging
# import graypy

# my_logger = logging.getLogger('test_logger')
# my_logger.setLevel(logging.DEBUG)

# handler = graypy.GELFUDPHandler('10.0.196.22', 81)
# my_logger.addHandler(handler)

# o = {
#     'app': 'holding_trusov.',
#     'source': 'onyx-sql-dev0',
#     'event': 'test python'
# }

# my_logger.debug(o)