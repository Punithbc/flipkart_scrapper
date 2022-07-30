# doing necessary imports
import threading
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

from logger_class import my_log
from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
import pandas as pd
from cassandra_db import CassandraManager
from FlipkratScrapping import FlipkratScrapper
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from cassandra_db import LOCATION, CLIENT_ID, CLIENT_SECRET
import time

rows = {}
searchString = None

# logger = my_log('flipkrat.py')

# free_status = True
db_name = 'Flipkart-Scrapper'

app = Flask(__name__)  # initialising the flask app with the name 'app'

# For selenium driver implementation on heroku
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("disable-dev-shm-usage")


# To avoid the time out issue on heroku
class threadClass:

    def __init__(self, expected_review, searchString, scrapper_object, review_count):
        self.expected_review = expected_review
        self.searchString = searchString
        self.scrapper_object = scrapper_object
        self.review_count = review_count
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        global free_status
        free_status = False
        self.scrapper_object.getReviewsToDisplay(expected_review=self.expected_review, searchString=self.searchString,
                                                 review_count=self.review_count)

        # logger.info_log("Thread run completed")
        free_status = True


@app.route('/', methods=['POST', 'GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        # print("jkjdflks")
        # global free_status
        # ## To maintain the internal server issue on heroku
        # if free_status != True:

        # else:
        #     free_status = True
        global searchString
        searchString = request.form['content'].replace(" ", "")  # obtaining the search string entered in the form
        expected_review = int(request.form['expected_review'])
        try:
            review_count = 0
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)
            cassandra_client = CassandraManager(LOCATION, CLIENT_ID, CLIENT_SECRET)
            cassandra_client.connect_session()
            scrapper_object.openUrl("https://www.flipkart.com/")
            # logger.info_log("Url hitted")
            scrapper_object.login_popup_handle()
            # logger.info_log("login popup handled")
            scrapper_object.searchProduct(searchString=searchString)
            # logger.info_log(f"Search begins for {searchString}")
            cassandra_client.create_table(table_name=searchString)
            time.sleep(2)
            if cassandra_client.check_table_is_present(table_name=searchString):
                time.sleep(2)
                response = cassandra_client.fetch_data_from_table(table_name=searchString)
                time.sleep(2)
                reviews = [i for i in response]

                if len(reviews) > expected_review:
                    result = [reviews[i] for i in range(0, expected_review)]
                    df = pd.DataFrame(result)
                    col = list(df.columns)
                    leng = int(expected_review)
                    scrapper_object.saveDataFrameToFile(file_name="static/scrapper_data.csv",
                                                        dataframe=df)
                    # logger.info_log("Data saved in scrapper file")
                    cassandra_client.create_table(table_name=searchString)
                    data_in_iterable_form = cassandra_client.fetch_data_from_table(table_name=searchString)
                    return render_template('results.html', rows=df, columns=col, length=leng)  # show the results to user
                else:
                    review_count = len(reviews)
                    threadClass(expected_review=expected_review, searchString=searchString,
                                scrapper_object=scrapper_object, review_count=review_count)
                    # logger.info_log("data saved in scrapper file")
                    return redirect(url_for('feedback', expected_review=expected_review))
            else:
                threadClass(expected_review=expected_review, searchString=searchString, scrapper_object=scrapper_object,
                            review_count=review_count)
                return redirect(url_for('feedback', expected_review=expected_review))

        except Exception as e:
            raise Exception("(app.py) - Something went wrong while rendering all the details of product.\n" + str(e))

    else:
        return render_template('index.html')


@app.route('/feedback/<expected_review>', methods=['GET'])
@cross_origin()
def feedback(expected_review):
    try:
        global searchString
        if searchString is not None:
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)
            cassandra_client = CassandraManager(LOCATION, CLIENT_ID, CLIENT_SECRET)
            print("searchstring before going to loop")
            time.sleep(100)
            data = cassandra_client.fetch_data_from_table(table_name=searchString)
            time.sleep(20)
            reviews = [i for i in data]
            dataframe = pd.DataFrame(reviews)
            columns = list(dataframe.columns)
            print(dataframe)
            scrapper_object.saveDataFrameToFile(file_name="static/scrapper_data.csv", dataframe=dataframe)
            length = int(expected_review)
            return render_template('results.html', rows=dataframe, columns=columns, length=length)
        else:
            return render_template('results.html', rows=None)
    except Exception as e:
        raise Exception("(feedback) - Something went wrong on retrieving feedback.\n" + str(e))


@app.route("/graph", methods=['GET'])
@cross_origin()
def graph():
    return redirect(url_for('plot_png'))


@app.route('/a', methods=['GET'])
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_figure():
    data = pd.read_csv("static/scrapper_data.csv")
    dataframe = pd.DataFrame(data=data)
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    xs = dataframe['product_searched']
    ys = dataframe['rating']
    axis.scatter(xs, ys)
    return fig


if __name__ == '__main__':
    app.run(debug=True)
