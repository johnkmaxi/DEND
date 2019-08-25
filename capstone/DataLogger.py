class DataProcessingLogger:
    """
    Object for tracking data processing
    :param: cwd, the parent directory where the log files will be stored.
            cwd/data/logs/log_file.log
    :param: names, list of strings, a log file for each name in the list is
            created
    """
    def __init__(self, cwd, names=['make_data']):
        # check for data/logs/
        if not os.path.isdir('{}/data/logs/'.format(cwd)):
            os.mkdir('{}/data/'.format(cwd))
            os.mkdir('{}/data/logs/'.format(cwd))
        # Create a custom logger for each data type to save
        # grid search data
        self.logger = logging.getLogger('processing')
        self.logger.setLevel(logging.INFO)

        loggers = [self.logger]
        self.names = names

        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H%M%S")

        for l, n in zip(loggers,names):
            handler = self.make_handler(cwd, n, now)
            l.addHandler(handler)
            l.propagate = False

    def make_handler(self, cwd, name, timestamp):
        """
        :param: cwd, str, current working directory
        :param: name, the name of the logger
        :param: now, str, timestamp
        """
        # Create handlers
        handler = logging.FileHandler('{}/data/logs/{}.log'.format(cwd, name+' '+timestamp), delay=True)
        # Create formatters and add it to handlers
        format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(format)
        return handler
