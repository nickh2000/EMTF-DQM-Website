#!/root/csctiming/csctimingenv/bin/python
import flask
import query
import logging


gunicorn_error_logger = logging.getLogger('gunicorn.error')


app = flask.Flask(__name__)

app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.DEBUG)
app.logger.debug('this will show in the log')

@app.route('/')
def index():
    return flask.render_template('index2.html')

@app.route('/results', methods=['GET'])
def result():

    get_params = flask.request.args.to_dict(flat = False)
    run_nums = get_params['nm'][0]
    plot_types = get_params['plot_types'][0]
    ds = get_params['dataset_types'][0]
    rc = get_params['run_class'][0]
    ls = get_params['ls'][0]

    df, runs = query.main(run_nums, ds, plot_types.split(","), rc, ls)
    result = {}
    result['df'] = df
    result['run_nums'] = runs
    result['plot_types'] = plot_types
    return flask.render_template('result2.html', result = result)

@app.route('/<path:filename>')
def download_file(filename):
    return flask.send_from_directory(f'./',
                               filename, as_attachment=True)

if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=5000)
