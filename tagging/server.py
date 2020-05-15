from flask import Flask, make_response
from flask_restful import Api
import logging
import sys

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    api = Api(app)
    app.config.from_object('config')
    logger = logging.getLogger('tagging')
    try:
        logging_level = os.environ.get("LOGLEVEL", logging.DEBUG)
        print(f"Setting log level to {logging_level}")
        logger.setLevel(logging_level)

        # create console handler and set level to debug
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging_level)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)
    except Exception as e:
        print("cannot setup logging: {}".format(str(e)))

    from .resources import Events, Event
    api.add_resource(Events, "/api/events")
    api.add_resource(Event,  "/api/event/<uid>")

    from .resources import TagSets, TagSet
    api.add_resource(TagSets, "/api/tagsets")
    api.add_resource(TagSet,  "/api/tagsets/<uid>")

    # connect-false because frameworks like uwsgi fork after app is obtained, and are not
    # fork-safe.
    app.db = MongoClient(app.config['MONGO_URL'], connect=False)[app.config['MONGO_DB_NAME']]