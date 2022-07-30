import logging as lg


class my_log:
    def __init__(self, logger):
        self.lg = lg
        self.format = '%(asctime)-8s %(name)-9s %(levelname)s %(message)s'
        self.config = self.lg.basicConfig(filename='E:\\task\\log_practice.log', level=lg.INFO, format=self.format)
        self.logger = self.lg.getLogger(logger)

    def info_log(self, nfo):
        return self.logger.info(nfo)

    def error_log(self, error):
        return self.logger.error(error)

    def warning_log(self, warning):
        return self.logger.warning(warning)

    def exception_log(self, ex):
        return self.logger.exception(ex)